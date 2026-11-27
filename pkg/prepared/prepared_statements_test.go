// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package prepared

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type PreparedStatementsSuite struct {
	suite.Suite
	pStmtState PreparedStatementState
}

func TestPreparedStatementTestSuite(t *testing.T) {
	suite.Run(t, &PreparedStatementsSuite{
		pStmtState: CreateProxyPreparedStmtsState([]string{}),
	})
}

func (p *PreparedStatementsSuite) BeforeTest(suiteName, testName string) {
	p.pStmtState = CreateProxyPreparedStmtsState([]string{})
}

func TestPreparedStatementMetricTags(t *testing.T) {
	testCases := []struct {
		name string
		stmt PreparedStatement
		want []string
	}{
		{
			name: "unnamed",
			stmt: CreatePreparedStatement("", "", "sql", []uint32{}),
			want: []string{"named:false", "autocached:false"},
		},
		{
			name: "named",
			stmt: CreatePreparedStatement("clientName", "proxyName", "sql", []uint32{}),
			want: []string{"named:true", "autocached:false"},
		},
		{
			name: "autocached",
			stmt: CreatePreparedStatement("", "proxyName", "sql", []uint32{}),
			want: []string{"named:false", "autocached:true"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.want, testCase.stmt.GetMetricTags())
		})
	}

	assert.Equal(t, "existing:false", preparedStmtExistingTag(false))
	assert.Equal(t, "existing:true", preparedStmtExistingTag(true))
	assert.Equal(t, "from_error:false", preparedStmtFromErrorTag(false))
	assert.Equal(t, "from_error:true", preparedStmtFromErrorTag(true))
	assert.Equal(t, "existed:false", preparedStmtExistedTag(false))
	assert.Equal(t, "existed:true", preparedStmtExistedTag(true))
}

func (p *PreparedStatementsSuite) TestAddConfirmedNamedStmt() {
	pStmt := CreatePreparedStatement("clientName", "proxyName", "sql", []uint32{})
	err := p.pStmtState.AddConfirmedNamedStmt(pStmt)
	assert.NoError(p.T(), err)
	assert.Equal(p.T(), pStmt, p.pStmtState.FindConfirmedNamedStmtFromClientName(pStmt.GetClientName()))

	err = p.pStmtState.AddConfirmedNamedStmt(pStmt)
	assert.Errorf(p.T(), err, "expected an error confirming an already exisiting named prepared statement")
	assert.Equal(p.T(), pStmt, p.pStmtState.FindConfirmedNamedStmtFromClientName(pStmt.GetClientName()))
}

func (p *PreparedStatementsSuite) TestAddConfirmedNamedStmt_Unnamed() {
	pStmt := CreatePreparedStatement("", "", "sql", []uint32{})
	err := p.pStmtState.AddConfirmedNamedStmt(pStmt)
	assert.ErrorContains(p.T(), err, "attempting to add a confirmed unnamed prepared statement")
	assert.Nil(p.T(), p.pStmtState.FindConfirmedNamedStmtFromClientName(pStmt.GetClientName()))
}

func (p *PreparedStatementsSuite) TestAddConfirmedNamedStmt_AutoCached() {
	pStmt := CreatePreparedStatement("", "proxyName", "sql", []uint32{})
	err := p.pStmtState.AddConfirmedNamedStmt(pStmt)
	assert.NoError(p.T(), err)
	assert.Equal(p.T(), pStmt, p.pStmtState.FindConfirmedNamedStmtFromClientName(pStmt.GetClientName()))

	// unlike client prepared statements, it is valid to overwrite existing auto-cached prepared statements
	err = p.pStmtState.AddConfirmedNamedStmt(pStmt)
	assert.NoError(p.T(), err)
	assert.Equal(p.T(), pStmt, p.pStmtState.FindConfirmedNamedStmtFromClientName(pStmt.GetClientName()))
}

func (p *PreparedStatementsSuite) TestConfirmPendingStmt() {
	unnamedPStmt := CreatePreparedStatement("", "", "sql", []uint32{})
	namedPStmt := CreatePreparedStatement("clientName", "proxyName", "sql", []uint32{})

	p.pStmtState.EnqueuePendingStmt(unnamedPStmt, false)
	p.pStmtState.EnqueuePendingStmt(namedPStmt, false)
	p.pStmtState.EnqueuePendingStmt(unnamedPStmt, false)

	confirmedStmt := p.pStmtState.ConfirmPendingStmt()
	assert.True(p.T(), PreparedStatementEquivalent(unnamedPStmt, confirmedStmt))
	assert.Nil(p.T(), p.pStmtState.FindConfirmedNamedStmtFromClientName(unnamedPStmt.GetClientName()))

	confirmedStmt = p.pStmtState.ConfirmPendingStmt()
	assert.True(p.T(), PreparedStatementEquivalent(namedPStmt, confirmedStmt))

	confirmedStmt = p.pStmtState.ConfirmPendingStmt()
	assert.True(p.T(), PreparedStatementEquivalent(unnamedPStmt, confirmedStmt))

	assert.True(p.T(), PreparedStatementEquivalent(namedPStmt, p.pStmtState.FindConfirmedNamedStmtFromClientName(namedPStmt.GetClientName())))
	assert.Nil(p.T(), p.pStmtState.FindConfirmedNamedStmtFromClientName(unnamedPStmt.GetClientName()))
}

func (p *PreparedStatementsSuite) TestConfirmPendingStmt_AutoCached() {
	// Regular named prepared statement
	namedPStmt := CreatePreparedStatement("clientName", "proxyName1", "SELECT * FROM table1", []uint32{})

	// Another named prepared statement
	namedPStmtTwo := CreatePreparedStatement("clientName2", "proxyName2", "SELECT * FROM table3", []uint32{})

	// Auto-cached unnamed prepared statement
	autoCachedPStmt := CreatePreparedStatement("", "proxyName3", "SELECT * FROM table2 WHERE id = $1", []uint32{})

	// Add another autocached statement with the same empty client name
	autoCachedPStmtTwo := CreatePreparedStatement("", "proxyName4", "SELECT * FROM table4 WHERE id = $1", []uint32{})

	// Queue mixed types of prepared statements
	p.pStmtState.EnqueuePendingStmt(namedPStmt, false)
	p.pStmtState.EnqueuePendingStmt(autoCachedPStmt, false)
	p.pStmtState.EnqueuePendingStmt(namedPStmtTwo, false)

	confirmedStmt := p.pStmtState.ConfirmPendingStmt()
	assert.True(p.T(), PreparedStatementEquivalent(namedPStmt, confirmedStmt))

	confirmedStmt = p.pStmtState.ConfirmPendingStmt()
	assert.True(p.T(), PreparedStatementEquivalent(autoCachedPStmt, confirmedStmt))

	confirmedStmt = p.pStmtState.ConfirmPendingStmt()
	assert.True(p.T(), PreparedStatementEquivalent(namedPStmtTwo, confirmedStmt))

	// Queue and confirm another auto-cached statement
	p.pStmtState.EnqueuePendingStmt(autoCachedPStmtTwo, false)
	confirmedStmt = p.pStmtState.ConfirmPendingStmt()

	// Verify it replaced the previous autocached statement with the same client name (empty)
	assert.True(p.T(), PreparedStatementEquivalent(autoCachedPStmtTwo, confirmedStmt))

	// The previous autocached statement should be overwritten
	foundStmt := p.pStmtState.FindConfirmedNamedStmtFromClientName("")
	assert.Equal(p.T(), "proxyName4", foundStmt.GetProxyName())
}

func (p *PreparedStatementsSuite) TestInvalidatePendingStmts() {
	unnamedPStmt := CreatePreparedStatement("", "proxyName", "sql", []uint32{})
	namedPStmt := CreatePreparedStatement("clientName", "proxyName", "sql", []uint32{})

	p.pStmtState.EnqueuePendingStmt(unnamedPStmt, false)
	p.pStmtState.EnqueuePendingStmt(namedPStmt, false)
	p.pStmtState.EnqueuePendingStmt(unnamedPStmt, false)

	p.pStmtState.InvalidatePendingStmts(false)

	confirmed := p.pStmtState.ConfirmPendingStmt()
	assert.Nil(p.T(), confirmed)
}

func (p *PreparedStatementsSuite) TestInvalidatePendingStmts_AutoCached() {
	// Regular named prepared statement
	namedPStmt := CreatePreparedStatement("clientName", "proxyName1", "SELECT * FROM table1", []uint32{})

	// Auto-cached unnamed prepared statement
	autoCachedPStmt := CreatePreparedStatement("", "proxyName2", "SELECT * FROM table2 WHERE id = $1", []uint32{})

	// First confirm some statements so they're in the confirmed map
	p.pStmtState.EnqueuePendingStmt(namedPStmt, false)
	p.pStmtState.EnqueuePendingStmt(autoCachedPStmt, false)

	p.pStmtState.InvalidatePendingStmts(false)

	// Verify all pending statements are gone
	confirmed := p.pStmtState.ConfirmPendingStmt()
	assert.Nil(p.T(), confirmed)
}

func (p *PreparedStatementsSuite) TestFindKnownNamedStmtFromClientName() {
	pStmtA := CreatePreparedStatement("a", "proxyName", "sql", []uint32{})
	pStmtB := CreatePreparedStatement("a", "proxyName", "sql", []uint32{})

	p.pStmtState.EnqueuePendingStmt(pStmtA, false)
	p.pStmtState.EnqueuePendingStmt(pStmtB, false)
	p.pStmtState.ConfirmPendingStmt()

	assert.True(p.T(), PreparedStatementEquivalent(pStmtA, p.pStmtState.FindKnownNamedStmtFromClientName(pStmtA.GetClientName())))
	assert.True(p.T(), PreparedStatementEquivalent(pStmtA, p.pStmtState.FindKnownNamedStmtFromClientName(pStmtB.GetClientName())))
}

func (p *PreparedStatementsSuite) TestFindKnownNamedStmtFromClientName_AutoCached() {
	// Named prepared statement
	namedPStmt := CreatePreparedStatement("clientName", "proxyName1", "SELECT * FROM table1", []uint32{})

	// Auto-cached unnamed prepared statement
	autoCachedPStmt := CreatePreparedStatement("", "proxyName2", "SELECT * FROM table2 WHERE id = $1", []uint32{})

	// Queue and confirm the named statement
	p.pStmtState.EnqueuePendingStmt(namedPStmt, false)
	p.pStmtState.ConfirmPendingStmt()

	// Queue but don't confirm the auto-cached statement
	p.pStmtState.EnqueuePendingStmt(autoCachedPStmt, false)

	// FindKnownNamedStmtFromClientName should find both confirmed and pending statements
	// For the named statement, it should find the confirmed one
	foundStmt := p.pStmtState.FindKnownNamedStmtFromClientName("clientName")
	assert.True(p.T(), PreparedStatementEquivalent(namedPStmt, foundStmt))

	// For the auto-cached statement, it should find the pending one
	foundStmt = p.pStmtState.FindKnownNamedStmtFromClientName("")
	assert.True(p.T(), PreparedStatementEquivalent(autoCachedPStmt, foundStmt))
}

func TestVerdictJudgedOnFirstCall(t *testing.T) {
	stmt := CreatePreparedStatement("", "", "SET search_path = app", nil)
	assert.False(t, Judged(stmt), "creating a statement scans nothing")
	assert.True(t, stmt.MayChangeSession())
	assert.True(t, Judged(stmt), "the verdict is cached")

	clean := CreatePreparedStatement("", "", "SELECT 1", nil)
	assert.False(t, clean.MayChangeSession())
	assert.True(t, Judged(clean), "a clean verdict is cached too")

	queued := createQueuedPreparedStmt(createQueuedPreparedStmt(stmt, true), false)
	assert.Same(t, stmt, Unwrap(queued), "Unwrap removes every queue wrapper")
	assert.True(t, Judged(queued))
}

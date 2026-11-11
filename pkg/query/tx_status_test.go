// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTxStatus(t *testing.T) {
	assert := assert.New(t)

	s, err := TxStatusFromByte('I')
	assert.NoError(err)
	assert.Equal(Idle, s)
	s, err = TxStatusFromByte('T')
	assert.NoError(err)
	assert.Equal(InTransaction, s)
	s, err = TxStatusFromByte('E')
	assert.NoError(err)
	assert.Equal(InErroredTransaction, s)
	_, err = TxStatusFromByte('?')
	assert.ErrorContains(err, "unexpected TxStatus: ?")
}

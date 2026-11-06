package mirror

import (
	"strings"
	"testing"

	"github.com/DataDog/pg-proxy/pkg/prepared"
)

func catalogStmt(name, query string) prepared.PreparedStatement {
	return prepared.CreatePreparedStatement("", name, query, nil)
}

// The entry cap bounds how many statements are held, not how large they are. Without a
// byte bound a session rotating large auto-cached queries retains 4096 of them, outside
// every mirror budget.
func TestStmtCatalog_EvictsOnBytes(t *testing.T) {
	c := newStmtCatalog()
	big := strings.Repeat("x", 1<<20) // 1 MiB each, so the byte cap binds long before 4096 entries

	for i := range 8 {
		c.put(catalogStmt(string(rune('a'+i)), big))
	}

	if c.bytes > mirrorStmtCatalogMaxBytes {
		t.Errorf("bytes = %d, want <= %d", c.bytes, mirrorStmtCatalogMaxBytes)
	}
	if len(c.order) >= 8 {
		t.Errorf("held %d entries, expected the byte cap to have evicted some", len(c.order))
	}
	if _, ok := c.get("a"); ok {
		t.Error("oldest entry survived, eviction is not in insertion order")
	}
	if _, ok := c.get("h"); !ok {
		t.Error("newest entry was not retained")
	}
}

// Accounting must survive eviction, or the running total drifts and the cap stops binding.
func TestStmtCatalog_ByteCountTracksContents(t *testing.T) {
	c := newStmtCatalog()
	for i := range 12 {
		c.put(catalogStmt(string(rune('a'+i)), strings.Repeat("y", 512<<10)))
	}

	want := 0
	for _, name := range c.order {
		st, ok := c.get(name)
		if !ok {
			t.Fatalf("%q in order but not in byName", name)
		}
		want += sizeOfStatement(st)
	}
	if c.bytes != want {
		t.Errorf("bytes = %d, want %d (sum of live entries)", c.bytes, want)
	}
	if len(c.sizes) != len(c.order) {
		t.Errorf("sizes has %d entries, order has %d: evicted sizes leaked", len(c.sizes), len(c.order))
	}
}

// A statement too large for the cap must not empty the catalog trying to fit it.
func TestStmtCatalog_OversizedStatementRefused(t *testing.T) {
	c := newStmtCatalog()
	c.put(catalogStmt("keep", "select 1"))
	c.put(catalogStmt("huge", strings.Repeat("z", mirrorStmtCatalogMaxBytes+1)))

	if _, ok := c.get("huge"); ok {
		t.Error("oversized statement was admitted")
	}
	if _, ok := c.get("keep"); !ok {
		t.Error("admitting an oversized statement evicted the existing entries")
	}
}

// The entry cap must still bind for small statements.
func TestStmtCatalog_EvictsOnEntries(t *testing.T) {
	c := newStmtCatalog()
	for i := range mirrorStmtCatalogMax + 10 {
		c.put(catalogStmt(string(rune(i)), "select 1"))
	}
	if len(c.order) != mirrorStmtCatalogMax {
		t.Errorf("held %d entries, want %d", len(c.order), mirrorStmtCatalogMax)
	}
}

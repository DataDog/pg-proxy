package mirror

import (
	"github.com/DataDog/pg-proxy/pkg/prepared"
)

// Entry backstop. A connection holds at most DefaultPreparedStmtsPerConnection.
const mirrorStmtCatalogMax = 4096

// Byte backstop. The entry cap alone bounds count, not size: a session rotating
// auto-cached queries can retain 4096 arbitrarily large SQL texts, which no mirror
// budget accounts for. Sized so an average statement set still fills the entry cap,
// leaving this to bind only when statements are unusually large.
const mirrorStmtCatalogMaxBytes = 4 << 20 // 4 MiB

// stmtCatalog remembers every named statement the session parsed, so the worker can
// re-parse one onto a shadow connection that lacks it. Owned by one goroutine, no lock.
// Bounded by entries and by bytes; whichever binds first evicts in insertion order.
type stmtCatalog struct {
	byName map[string]prepared.PreparedStatement
	order  []string // insertion order, for eviction
	sizes  map[string]int
	bytes  int
}

func newStmtCatalog() *stmtCatalog {
	return &stmtCatalog{
		byName: make(map[string]prepared.PreparedStatement),
		sizes:  make(map[string]int),
	}
}

// sizeOfStatement approximates what an entry retains: the SQL text dominates, and the
// OID slice is charged at its wire width since the clone keeps it alive.
func sizeOfStatement(st prepared.PreparedStatement) int {
	return len(st.GetProxyName()) + len(st.GetSQLQuery()) + oidWireSize*len(st.GetParamOIDs())
}

func (c *stmtCatalog) put(st prepared.PreparedStatement) {
	name := st.GetProxyName()
	if _, ok := c.byName[name]; ok {
		return
	}
	size := sizeOfStatement(st)
	// A single statement larger than the whole cap would evict everything and still
	// not fit, so refuse it instead: a catalog_miss costs one batch, an empty catalog
	// costs the session.
	if size > mirrorStmtCatalogMaxBytes {
		return
	}
	for len(c.order) > 0 && (len(c.order) >= mirrorStmtCatalogMax || c.bytes+size > mirrorStmtCatalogMaxBytes) {
		c.evictOldest()
	}
	c.byName[name] = st
	c.sizes[name] = size
	c.bytes += size
	c.order = append(c.order, name)
}

func (c *stmtCatalog) evictOldest() {
	oldest := c.order[0]
	c.bytes -= c.sizes[oldest]
	delete(c.sizes, oldest)
	delete(c.byName, oldest)
	c.order = c.order[1:]
}

func (c *stmtCatalog) get(name string) (prepared.PreparedStatement, bool) {
	st, ok := c.byName[name]
	return st, ok
}

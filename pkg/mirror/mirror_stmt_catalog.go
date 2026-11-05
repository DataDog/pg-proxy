package mirror

import (
	"github.com/DataDog/pg-proxy/pkg/prepared"
)

// Memory backstop. A connection holds at most DefaultPreparedStmtsPerConnection.
const mirrorStmtCatalogMax = 4096

// stmtCatalog remembers every named statement the session parsed, so the worker can
// re-parse one onto a shadow connection that lacks it. Owned by one goroutine, no lock.
type stmtCatalog struct {
	byName map[string]prepared.PreparedStatement
	order  []string // insertion order, for eviction
}

func newStmtCatalog() *stmtCatalog {
	return &stmtCatalog{byName: make(map[string]prepared.PreparedStatement)}
}

func (c *stmtCatalog) put(st prepared.PreparedStatement) {
	name := st.GetProxyName()
	if _, ok := c.byName[name]; ok {
		return
	}
	if len(c.order) >= mirrorStmtCatalogMax {
		delete(c.byName, c.order[0])
		c.order = c.order[1:]
	}
	c.byName[name] = st
	c.order = append(c.order, name)
}

func (c *stmtCatalog) get(name string) (prepared.PreparedStatement, bool) {
	st, ok := c.byName[name]
	return st, ok
}

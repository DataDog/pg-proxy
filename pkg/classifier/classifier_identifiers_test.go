package classifier

import (
	"fmt"
	"strings"
	"testing"
)

// The standalone parser uses PostgreSQL's default identifier limit. The private
// consumer can compile a parser with a larger limit. Routing decisions must be
// stable for identifiers around both limits, even when the parse tree truncates
// the name differently. This does not promise complete classifier equivalence.
func TestClassifyLongIdentifiers(t *testing.T) {
	for _, length := range []int{63, 64, 255, 256} {
		for _, character := range []string{"a", "é"} {
			name := strings.Repeat(character, length)
			cases := []struct {
				name string
				sql  string
				want Classification
			}{
				{"table", fmt.Sprintf(`SELECT * FROM "%s"`, name), Classification{ReadOnly: true, StatementCount: 1}},
				{"column", fmt.Sprintf(`SELECT "%s" FROM items`, name), Classification{ReadOnly: true, StatementCount: 1}},
				{"function", fmt.Sprintf(`SELECT "%s"()`, name), Classification{StatementCount: 1}},
				{"schema", fmt.Sprintf(`SELECT "%s".count(*) FROM items`, name), Classification{StatementCount: 1}},
				{"write CTE", fmt.Sprintf(`WITH "%s" AS (DELETE FROM items RETURNING *) SELECT * FROM "%s"`, name, name), Classification{StatementCount: 1}},
				{"multiple reads", fmt.Sprintf(`SELECT * FROM "%s"; SELECT * FROM "%s"`, name, name), Classification{ReadOnly: true, StatementCount: 2}},
			}
			for _, tc := range cases {
				t.Run(fmt.Sprintf("%s/%d/%s", character, length, tc.name), func(t *testing.T) {
					got, err := Classify(tc.sql)
					if err != nil {
						t.Fatal(err)
					}
					if got != tc.want {
						t.Fatalf("Classify() = %+v, want %+v", got, tc.want)
					}
				})
			}
		}
	}
}

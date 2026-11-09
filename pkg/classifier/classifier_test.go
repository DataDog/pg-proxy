package classifier

import "testing"

func TestClassifyReadOnly(t *testing.T) {
	queries := map[string]string{
		"simple select":                   "SELECT * FROM dogs WHERE name = 'Fido';",
		"select join":                     "SELECT t.oid FROM pg_type t JOIN pg_namespace ns ON typnamespace = ns.oid;",
		"select CTE":                      "WITH fidos AS (SELECT * FROM dogs WHERE name = 'Fido') SELECT id FROM fidos;",
		"multiple CTEs":                   "WITH old_dogs AS (SELECT * FROM dogs), living AS (SELECT * FROM old_dogs) SELECT * FROM living;",
		"nested CTEs":                     "WITH old_dogs AS (WITH dogs AS (SELECT * FROM dogs) SELECT * FROM dogs) SELECT * FROM old_dogs;",
		"show":                            "SHOW statement_timeout;",
		"multiple reads":                  "SELECT * FROM dogs; SELECT * FROM cats;",
		"read-only tx":                    "BEGIN READ ONLY;",
		"read-only tx with isolation":     "BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY;",
		"at time zone":                    "SELECT name, check_in AT TIME ZONE 'UTC' FROM dogs;",
		"timezone call":                   "SELECT timezone('UTC', now);",
		"count star":                      "SELECT COUNT(*) FROM dogs;",
		"count number":                    "SELECT COUNT(1) FROM dogs;",
		"count column":                    "SELECT COUNT(name) FROM dogs WHERE age > 5;",
		"count group":                     "SELECT breed, COUNT(*) FROM dogs GROUP BY breed;",
		"sum":                             "SELECT SUM(age) FROM dogs;",
		"avg":                             "SELECT AVG(age) FROM dogs;",
		"min":                             "SELECT MIN(age) FROM dogs;",
		"max":                             "SELECT MAX(age) FROM dogs;",
		"string_agg":                      "SELECT STRING_AGG(name, ', ') FROM dogs;",
		"array_agg":                       "SELECT ARRAY_AGG(name) FROM dogs GROUP BY breed;",
		"bool_and":                        "SELECT BOOL_AND(is_vaccinated) FROM dogs;",
		"bool_or":                         "SELECT BOOL_OR(is_good_boy) FROM dogs;",
		"json_agg":                        "SELECT JSON_AGG(name) FROM dogs;",
		"jsonb_agg":                       "SELECT JSONB_AGG(name) FROM dogs;",
		"json_build_object":               "SELECT JSON_BUILD_OBJECT('name', name) FROM dogs;",
		"jsonb_build_object":              "SELECT JSONB_BUILD_OBJECT('name', name) FROM dogs;",
		"json_agg of json_build_object":   "SELECT JSON_AGG(JSON_BUILD_OBJECT('name', name)) FROM dogs;",
		"jsonb_agg of jsonb_build_object": "SELECT JSONB_AGG(JSONB_BUILD_OBJECT('name', name)) FROM dogs;",
	}

	for name, sql := range queries {
		t.Run(name, func(t *testing.T) {
			got, err := Classify(sql)
			if err != nil {
				t.Fatalf("Classify() error = %v", err)
			}
			if !got.ReadOnly {
				t.Fatal("Classify().ReadOnly = false, want true")
			}
		})
	}
}

func TestClassifyNotReadOnly(t *testing.T) {
	queries := map[string]string{
		"select for update":            "SELECT * FROM dogs WHERE name = 'Fido' FOR UPDATE;",
		"insert":                       "INSERT INTO dogs (id, name) VALUES (1, 'Fido');",
		"update":                       "UPDATE dogs SET upvotes = upvotes + 1 WHERE id = 1;",
		"delete":                       "DELETE FROM dogs WHERE id = 1;",
		"copy":                         "COPY dogs FROM '/path.csv' DELIMITER ',' CSV;",
		"locking CTE":                  "WITH var AS (SELECT id FROM dogs) SELECT id FROM var FOR SHARE;",
		"write outer CTE":              "WITH new_dogs AS (SELECT 1 AS id) INSERT INTO dogs (id) SELECT id FROM new_dogs;",
		"write CTE":                    "WITH changed AS (INSERT INTO dogs (id) VALUES (1) RETURNING *) SELECT * FROM changed;",
		"nested write CTE":             "WITH outer_cte AS (WITH inner_cte AS (INSERT INTO dogs (id) VALUES (1) RETURNING *) SELECT * FROM inner_cte) SELECT * FROM outer_cte;",
		"mixed statements":             "SELECT * FROM dogs; INSERT INTO cats (id) VALUES (1);",
		"unknown function":             "SELECT set_host_tags();",
		"select into":                  "SELECT * INTO new_table FROM old_table;",
		"begin transaction":            "BEGIN;",
		"read-write tx":                "BEGIN READ WRITE;",
		"read-write tx with isolation": "BEGIN ISOLATION LEVEL REPEATABLE READ, READ WRITE;",
		"read-write tx with select":    "BEGIN READ WRITE; SELECT 1;",
	}

	for name, sql := range queries {
		t.Run(name, func(t *testing.T) {
			got, err := Classify(sql)
			if err != nil {
				t.Fatalf("Classify() error = %v", err)
			}
			if got.ReadOnly {
				t.Fatal("Classify().ReadOnly = true, want false")
			}
		})
	}
}

func TestClassifyStatementCount(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want int
	}{
		{name: "empty tree", sql: "-- comment only", want: 0},
		{name: "one statement", sql: "SELECT 1", want: 1},
		{name: "multiple statements", sql: "SELECT 1; INSERT INTO dogs VALUES (1)", want: 2},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Classify(test.sql)
			if err != nil {
				t.Fatalf("Classify() error = %v", err)
			}
			if got.StatementCount != test.want {
				t.Fatalf("Classify().StatementCount = %d, want %d", got.StatementCount, test.want)
			}
		})
	}
}

func TestClassifyMalformedQuery(t *testing.T) {
	if _, err := Classify("SELECT * FROM dogs WHERE;"); err == nil {
		t.Fatal("Classify() error = nil, want parse error")
	}
}

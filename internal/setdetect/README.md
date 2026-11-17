# setdetect: detecting session-changing SQL for `reset_mode: dynamic`

An experiment, not production code. It compares ways to decide from the
text of a Query or Parse message whether the checkout may have changed
session state that the release reset (`RESET SESSION AUTHORIZATION; RESET
ROLE; RESET ALL`) restores. Nothing in `pkg/` imports it.

## Layout

| Path | What |
|---|---|
| `detector.go`, `registry.go` | The `Detector` interface (`Classify(sql, scsOn) Verdict`) and the list of approaches |
| `simple.go` | Substring (the proposal's rule) and word-boundary baselines |
| `pgdog.go` | Port of pgdog 96c83f3: regex gate, `query_parser: auto`, `query_parser: on` |
| `proxysql.go` | Port of ProxySQL v3.0 663e84a, default configuration |
| `pgquery.go`, `astwalk.go`, `cache.go` | pg_query (cgo): full parse, prefilter + parse, scanner + rules, verdict cache |
| `lexer.go` | Hand-written token-aware lexer |
| `twostage.go` | Cheap first stage (byte loop, SWAR, Aho-Corasick DFA), then the lexer |
| `purego/` | CockroachDB's pure-Go parser, in its own module so it stays out of `go.mod` |
| `corpus/corpus.jsonl` | The labeled corpus (one case per line) |
| `corpus/oracle-pg14.jsonl`, `corpus/oracle-pg16.jsonl` | What executing each case did to session state |
| `corpus/labeler-vs-hand.txt` | Where the rule labeler and the hand labels disagree (expected: context the rules cannot see) |
| `gen/` | The corpus generator: rule labeler, string-literal extraction, hand-labeled synthetic cases |
| `oracle_test.go`, `oracle_fixture.sql` | The execution oracle (build tags `integration,oracle`) |
| `accuracy_test.go` | Confusion matrices, false negatives, false positive rates, golden verdict hashes |
| `fuzz_test.go`, `reference_test.go` | Lexer vs AST fuzzing; optimized vs pre-optimization equivalence fuzzing |
| `bench_test.go` | Size-class, traffic, cgo, before/after benchmarks and the latency test |
| `results/` | Raw outputs: accuracy, latency, benchstat, profiles, optimization log |

## Regenerating

```sh
# 1. Corpus. Sources are optional; each missing one drops its cases.
go run ./internal/setdetect/gen \
  -regress <postgresql-16.15>/src/test/regress \
  -proxysql <proxysql> -pgdog <pgdog> -pgbouncer <pgbouncer>

# 2. Oracle, once per server version (about 15 minutes each).
ORACLE_IMAGE=<image> ORACLE_TAG=pg16 ORACLE_REGRESS=<postgresql-16.15>/src/test/regress \
  go test -tags integration,oracle -run TestOracle -timeout 90m ./internal/setdetect

# 3. Re-run step 1: the generator folds oracle results into labels that
#    depend on code the text does not show (EXECUTE, CALL, DO with dynamic SQL).

# 4. Pure-Go parser verdicts, then accuracy.
(cd internal/setdetect/purego && CGO_ENABLED=0 go test -run TestVerdicts .)
go test -run TestAccuracy ./internal/setdetect -write   # add -update after a deliberate rule change

# 5. Fuzzing.
go test -run '^$' -fuzz FuzzLexerVsAST -fuzztime 10m ./internal/setdetect
go test -run '^$' -fuzz FuzzOptimizedEqualsReference -fuzztime 10m ./internal/setdetect

# 6. Benchmarks.
GOMAXPROCS=1 go test -run '^$' -bench 'Classify|Traffic|Cgo' -benchmem -count=10 ./internal/setdetect > results/bench-final.txt
benchstat results/bench-final.txt
LATENCY=1 go test -run TestLatency ./internal/setdetect
ORACLE_IMAGE=<image> go test -tags integration,oracle -run TestServerReference ./internal/setdetect
```

## Labels

Each case is `dirty`, `clean` or `acceptable-either`, with a category (P1 to
P9 dirty, N1 to N7 clean; see `categories.go`) and a source.

- Hand-labeled cases (`synth-*` sources) carry the author's label from the
  category rules.
- Cases from PostgreSQL's regression suite and other projects' tests get a
  label from the rule labeler in `gen/label.go`, which works on the
  pg_query parse tree. Where the label depends on code the text does not
  show (EXECUTE of a prepared statement, CALL, a DO block that runs dynamic
  SQL), the oracle decides. A rule-clean statement that the oracle saw
  change state becomes P9 (server-side code).
- The oracle runs every executable case in a fresh session (regression
  scripts run in schedule order, one session per script) and compares
  `pg_settings WHERE source = 'session'`, `current_user`, `session_user` and
  every custom setting name in the corpus before and after.

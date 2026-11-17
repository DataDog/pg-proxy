# Optimization log

Every pass was checked two ways:

- **Golden verdicts:** TestAccuracy compares per-detector verdict hashes in `verdicts.json`.
- **Equivalence fuzzing:** FuzzOptimizedEqualsReference runs the optimized code against the pre-optimization copies in `reference_test.go`, seeded with the whole corpus, then for 10 minutes and 11.6M inputs on the final code.

The fuzzer rejected two candidate changes, and both were reworked until verdicts matched:

- **Lexer number skip:** the lexer's fast skip treated digits as single bytes. The original consumes `00$$` as one number-like token; the fast skip saw a dollar quote.
- **ProxySQL early exit:** it did not skip a leading `::type` cast, which the digest drops.

The before and after numbers come from `bench-beforeafter.txt` / `benchstat-beforeafter.txt`. Both sides ran in the same process, pinned to one core, with `GOMAXPROCS=1`, `-count=10` and medians. Where a pass changed several size classes, the table shows the most informative one; benchstat has all six.

| Approach | Pass (profile finding → change) | Class | Before | After | Outcome |
|---|---|---|---|---|---|
| substring | byte loop → SWAR search for "et" pairs, then check the "s" | 10 KB IN list | 6.62 µs | 3.72 µs | kept (−44%; +8 ns on 8-byte inputs, where the old loop exits at once) |
| word-boundary | same SWAR search, boundary check on each hit | ~200 B SELECT | 132.5 ns | 81.2 ns | kept (−39%) |
| pgdog-regex | 99% of time in Go regexp on comment-led text → byte loop with RE2 semantics (lazy comment end, ASCII \s) | ~200 B SELECT | 4.66 µs | 194 ns | kept (−96%) |
| pgdog-auto | gate shares the byte loop; parse is 67% cgo, 30% protobuf decode | multi-statement | (gate as above) | 73 µs | parse not reducible here |
| pgdog-on | 67% runtime.cgocall, 30% protobuf unmarshal, 59% of bytes in reflect.New | ~200 B SELECT | 108 µs | 108 µs | profiled; nothing to remove at this layer |
| proxysql | digest built for every query, 2 allocs → verdict-preserving early exit on the first word | ~200 B SELECT | 1.54 µs | 757 ns | kept (−51%, 352 B → 0 B) |
| proxysql | SET path: 19 allocs in the parse1v2 regex | SET | 7.79 µs | 7.08 µs | not pursued (SETs are rare; within noise) |
| pgquery-parse | 30% in the protoreflect walk → walk only when the text names a setter, EXECUTE or U& | ~400 B UPDATE | 198.6 µs | 145.5 µs | kept (−27%); rest is cgo and protobuf |
| pgquery-prefilter | switch per byte → first-byte table, plus the walk skip | ~200 B SELECT | 317 ns | 194 ns | kept (−39%) |
| pgquery-scan | 57% protobuf unmarshal of tokens, 28% cgocall | ~200 B SELECT | 18.5 µs | 18.5 µs | profiled; nothing to remove at this layer |
| pgquery-parse+cache | mutex around each slot → one atomic word per slot | ~200 B SELECT | 46.6 ns | 36.6 ns | kept (−21%) |
| lexer | pass 1: 22% in isIdentByte, digits and spaces one by one → identifier table, skip runs of punctuation and numbers past the first words | 10 KB IN list | 13.65 µs | 8.27 µs | kept (−39%) |
| lexer | pass 2: every body word compared → only words of length 7, 9, 10 or 11 | ~400 B UPDATE | 740 ns | 613 ns | kept (−17%, passes 1+2) |
| lexer | pass 3: stop-byte table (skip everything but quote, $, comment starts, ;, s/p/e/u) | 10 KB IN list | 7.1 µs | 14.2 µs | rejected (slower; quick run) |
| two-stage | inherits the lexer passes | ~200 B SELECT | 616 ns | 338 ns | kept |
| two-stage-swar | three masks → two ('t' and 'x' differ in two bits) | 10 KB IN list | 12.40 µs | 5.79 µs | kept (with lexer passes) |
| two-stage-dfa | one dependent table load per byte | 10 KB IN list | 27.3 µs | 27.3 µs | profiled; inherent, no pass |
| crdb-parser | 84% inside the goyacc parser | ~200 B SELECT | 78 µs | 78 µs | profiled; third-party parser, no pass |

Stopping rule: a pass that gained under 10% ended work on that approach. The lexer stopped after pass 3 failed, and the SWAR stage after one pass.

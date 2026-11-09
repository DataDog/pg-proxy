// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import "fmt"

// Priority is a portable workload priority for a session's per-query
// admission control. The zero value is deliberately unset — it is NOT P0 —
// so a caller that forgets to set a Priority cannot silently obtain the
// highest priority. Use one of P0..P5, or construct one from an untrusted
// numeric value with PriorityFromLevel. Read the numeric level with
// Level(), which folds an unset Priority to DefaultPriority: a parser
// regression that never assigns this field yields P3 at the load shedder,
// never a silent P0.
type Priority struct {
	level *uint32
}

// maxPriorityLevel is the lowest (least urgent) defined priority level.
const maxPriorityLevel = 5

// Level returns the numeric priority level (0 = highest, 5 = lowest),
// folding an unset Priority (the zero value) to DefaultPriority.
func (p Priority) Level() uint32 {
	if p.level == nil {
		return *DefaultPriority.level
	}
	return *p.level
}

func priorityLevel(n uint32) Priority {
	return Priority{level: &n}
}

// P0 is the highest priority; P5 is the lowest.
var (
	P0 = priorityLevel(0)
	P1 = priorityLevel(1)
	P2 = priorityLevel(2)
	P3 = priorityLevel(3)
	P4 = priorityLevel(4)
	P5 = priorityLevel(5)
)

// DefaultPriority is the default priority level used when a client does not
// specify a priority via the pg_proxy.priority option.
var DefaultPriority = P3

// PriorityFromLevel validates that n is a defined priority level (0..5) and
// returns the corresponding Priority. It is the only way to construct a
// Priority from an untrusted numeric value, such as a parsed client option.
func PriorityFromLevel(n uint32) (Priority, error) {
	if n > maxPriorityLevel {
		return Priority{}, fmt.Errorf("invalid priority level %d: must be between 0 and %d", n, maxPriorityLevel)
	}
	return priorityLevel(n), nil
}

// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package session

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPriorityLevel(t *testing.T) {
	tests := map[string]struct {
		priority Priority
		want     uint32
	}{
		"zero value folds to DefaultPriority": {
			priority: Priority{},
			want:     3,
		},
		"P3 reads back as level 3": {
			priority: P3,
			want:     3,
		},
		"P0 reads back as level 0": {
			priority: P0,
			want:     0,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.want, test.priority.Level())
		})
	}
}

func TestPriorityFromLevel(t *testing.T) {
	p3, err := PriorityFromLevel(3)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), p3.Level())

	p0, err := PriorityFromLevel(0)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), p0.Level())

	_, err = PriorityFromLevel(6)
	assert.Error(t, err)
}

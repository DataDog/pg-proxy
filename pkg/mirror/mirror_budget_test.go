// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package mirror

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMirrorBudgetForMemory(t *testing.T) {
	// Non-positive or small pods clamp up to the floor.
	assert.Equal(t, mirrorBudgetFloorBytes, mirrorBudgetForMemory(0))
	assert.Equal(t, mirrorBudgetFloorBytes, mirrorBudgetForMemory(-1))
	assert.Equal(t, mirrorBudgetFloorBytes, mirrorBudgetForMemory(100*1024*1024), "small pod clamps to floor")

	// A large pod gets the fraction of its hard limit, above the floor.
	var hard int64 = 16 * 1024 * 1024 * 1024 // 16 GiB
	assert.Equal(t, int64(float64(hard)*mirrorBudgetMemFraction), mirrorBudgetForMemory(hard))
	assert.Greater(t, mirrorBudgetForMemory(hard), mirrorBudgetFloorBytes)
}

func TestMirrorBudget_ReserveAndRelease(t *testing.T) {
	b := newMirrorBudget(100)

	assert.True(t, b.tryReserve(60))
	assert.EqualValues(t, 60, b.UsedBytes())
	assert.True(t, b.tryReserve(40), "reserving exactly up to the cap is allowed")
	assert.EqualValues(t, 100, b.UsedBytes())

	assert.False(t, b.tryReserve(1), "over-cap reserve is rejected")
	assert.EqualValues(t, 100, b.UsedBytes(), "a rejected reserve must not change usage")

	b.release(100)
	assert.EqualValues(t, 0, b.UsedBytes())
	assert.True(t, b.tryReserve(100), "space freed by release is reusable")
}

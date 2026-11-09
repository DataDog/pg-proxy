// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package warden

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewConcurrentMap(t *testing.T) {
	m := map[string]string{"foo": "bar"}
	cm := NewConcurrentMap(m)

	assert.Equal(t, m, cm.innerMap)
}

func TestConcurrentMapAll(t *testing.T) {
	m := map[string]string{"foo": "bar", "baz": "qux"}
	cm := NewConcurrentMap(m)

	assert.Equal(t, m, maps.Collect(cm.All()))
}

func TestConcurrentMapPut(t *testing.T) {
	cm := NewConcurrentMap(map[string]string{})
	cm.Put("foo", "bar")

	assert.Equal(t, "bar", cm.innerMap["foo"])
}

func TestConcurrentMapGet(t *testing.T) {
	cm := NewConcurrentMap(map[string]string{"foo": "bar"})
	result, ok := cm.Get("foo")

	assert.True(t, ok)
	assert.Equal(t, "bar", result)
}

func TestConcurrentMapSet(t *testing.T) {
	cm := NewConcurrentMap(map[string]string{"foo": "bar"})
	m := map[string]string{"baz": "qux"}
	cm.Set(m)

	assert.Equal(t, m, cm.innerMap)
}

func TestConcurrentMapDelete(t *testing.T) {
	cm := NewConcurrentMap(map[string]string{"foo": "bar"})
	cm.Delete("foo")
	_, ok := cm.Get("foo")

	assert.False(t, ok)
}

func TestConcurrentMapLength(t *testing.T) {
	cm := NewConcurrentMap(map[string]string{"foo": "bar"})
	length := cm.Length()

	assert.Equal(t, 1, length)
}

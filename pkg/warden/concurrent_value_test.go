package warden

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewConcurrentValue(t *testing.T) {
	cv := NewConcurrentValue(123)

	assert.Equal(t, 123, cv.value)
}

func TestBorrow(t *testing.T) {
	cv := NewConcurrentValue(123)
	release := cv.Borrow()
	written := make(chan struct{})
	go func() {
		cv.Set(456)
		close(written)
	}()
	select {
	case <-written:
		t.Error("Set completed while the value was borrowed")
	case <-time.After(10 * time.Millisecond):
	}
	release()
	select {
	case <-written:
		assert.Equal(t, 456, cv.Get())
	case <-time.After(time.Second):
		t.Fatal("Set did not complete after release")
	}
}

func TestConcurrentValueGet(t *testing.T) {
	cv := NewConcurrentValue(123)

	assert.Equal(t, 123, cv.Get())
}

func TestConcurrentValueSet(t *testing.T) {
	cv := NewConcurrentValue(123)
	cv.Set(456)

	assert.Equal(t, 456, cv.Get())
}

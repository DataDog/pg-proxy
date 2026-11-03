package pool

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

type stubPool struct{ acquired bool }

func (p *stubPool) Acquire(context.Context) (*pgxpool.Conn, error) {
	p.acquired = true
	return nil, nil
}
func (*stubPool) ExportStats([]string) {}
func (*stubPool) Close()               {}
func (*stubPool) Reset()               {}

type stubDirectPool struct{ err error }

func (p *stubDirectPool) AcquireConnection(context.Context) (*pgxpool.Conn, error) {
	return nil, p.err
}
func (*stubDirectPool) ClosePools()            {}
func (*stubDirectPool) ExportDirectPoolStats() {}
func (*stubDirectPool) Reset()                 {}

func TestFallbackPoolUsesPrimaryAfterDirectFailure(t *testing.T) {
	primary := &stubPool{}
	directErr := errors.New("direct unavailable")
	var observed error
	pool := &FallbackPool{
		Primary: primary,
		Direct:  &stubDirectPool{err: directErr},
		Hooks: FallbackHooks{
			DirectAcquireFailed: func(err error) { observed = err },
		},
	}

	_, err := pool.Acquire(context.Background())
	assert.NoError(t, err)
	assert.True(t, primary.acquired)
	assert.ErrorIs(t, observed, directErr)
}

func TestPGXPoolRecoversUninitializedAcquire(t *testing.T) {
	pool := NewPGXPool(&pgxpool.Pool{}, PGXHooks{})
	_, err := pool.Acquire(context.Background())
	assert.EqualError(t, err, "recovered from panicking: failed to acquire connection from pgxpool.Pool")
}

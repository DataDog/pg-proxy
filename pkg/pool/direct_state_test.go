package pool

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stateTestInstance struct{ healthy bool }

func (*stateTestInstance) AcquireConnection(context.Context) (*pgxpool.Conn, error) { return nil, nil }
func (*stateTestInstance) HealthCheck()                                             {}
func (i *stateTestInstance) IsHealthy() bool                                        { return i.healthy }
func (*stateTestInstance) Close()                                                   {}
func (*stateTestInstance) Stat() *pgxpool.Stat                                      { return nil }
func (*stateTestInstance) Reset()                                                   {}

func TestDirectStateSelectsHealthyInstances(t *testing.T) {
	state := NewDirectState()
	state.Store("10.0.0.2", &stateTestInstance{})
	state.Store("10.0.0.1", &stateTestInstance{healthy: true})
	assert.Equal(t, []string{"10.0.0.1"}, state.RefreshHealthy())

	address, index, err := state.NextHealthyAddress("db.test")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.1", address)
	assert.Equal(t, 0, index)
}

func TestDirectStateNoHealthyInstances(t *testing.T) {
	_, _, err := NewDirectState().NextHealthyAddress("db.test")
	assert.ErrorIs(t, err, ErrNoHealthyInstances)
}

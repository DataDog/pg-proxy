package pool

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func TestPGKPoolConsumesHealthUpdates(t *testing.T) {
	updates := make(chan *PoolHealth, 1)
	setupStarted := make(chan struct{}, 1)
	pool := NewPGKPool(PGKPoolConfig{
		Direct:        NewDirectRuntime(DirectRuntimeConfig{}),
		Discover:      func() []string { return nil },
		HealthUpdates: updates,
		RunSetupWithRetry: func(setup func() error) {
			setupStarted <- struct{}{}
			_ = setup()
		},
		Start: true,
	})
	t.Cleanup(pool.ClosePools)

	updates <- &PoolHealth{}
	select {
	case <-setupStarted:
	case <-time.After(time.Second):
		t.Fatal("PGK pool did not react to the health update")
	}
}

func TestPGKPoolReconcilesDiscoveredInstances(t *testing.T) {
	direct := NewDirectRuntime(DirectRuntimeConfig{DatabaseHost: "postgres.test"})
	direct.State().Store("10.0.0.1", NewMockInstance(
		"10.0.0.1",
		&pgxpool.Pool{},
		nil,
		func(string) bool { return false },
	))

	runtime := NewPGKPool(PGKPoolConfig{
		Direct:      direct,
		Discover:    func() []string { return []string{"10.0.0.2"} },
		NewInstance: healthyMockInstance,
		Initialize:  true,
	})

	_, staleFound := runtime.State().Load("10.0.0.1")
	_, discoveredFound := runtime.State().Load("10.0.0.2")
	assert.False(t, staleFound)
	assert.True(t, discoveredFound)
	assert.ElementsMatch(t, []string{"10.0.0.2"}, runtime.State().HealthyAddresses())
}

func TestPGKPoolReportsLeaderState(t *testing.T) {
	addresses := []string{"10.0.0.1"}
	changed := 0
	unchanged := 0
	runtime := NewPGKPool(PGKPoolConfig{
		Direct:   NewDirectRuntime(DirectRuntimeConfig{DatabaseHost: "postgres.test"}),
		Discover: func() []string { return addresses },
		NewInstance: func(address string) PostgresInstance {
			return NewMockInstance(address, &pgxpool.Pool{}, nil, func(ip string) bool { return ip == addresses[0] })
		},
		LeaderChanged: func(_, _ string) {
			changed++
		},
		LeaderUnchanged: func(string) {
			unchanged++
		},
		Initialize: true,
	})
	t.Cleanup(runtime.ClosePools)

	assert.NoError(t, runtime.Setup())
	assert.Zero(t, changed)
	assert.Equal(t, 1, unchanged)

	addresses = []string{"10.0.0.2"}
	assert.NoError(t, runtime.Setup())
	assert.Equal(t, 1, changed)
	assert.Equal(t, 1, unchanged)
}

func TestPGKPoolKeepsLastHealthyInstanceDuringDiscoveryGap(t *testing.T) {
	addresses := []string{"10.0.0.1"}
	runtime := NewPGKPool(PGKPoolConfig{
		Direct:      NewDirectRuntime(DirectRuntimeConfig{DatabaseHost: "postgres.test"}),
		Discover:    func() []string { return addresses },
		NewInstance: healthyMockInstance,
		Initialize:  true,
	})

	addresses = nil
	assert.NoError(t, runtime.Setup())
	_, found := runtime.State().Load("10.0.0.1")
	assert.True(t, found)
}

func healthyMockInstance(address string) PostgresInstance {
	return NewMockInstance(address, &pgxpool.Pool{}, nil, func(string) bool { return true })
}

func TestPGKPoolReportsSetupTimingOnlyForRetryAttempts(t *testing.T) {
	for _, healthy := range []bool{false, true} {
		t.Run(map[bool]string{false: "unhealthy", true: "healthy"}[healthy], func(t *testing.T) {
			reports := 0
			attempts := 0
			pool := NewPGKPool(PGKPoolConfig{
				Direct:   NewDirectRuntime(DirectRuntimeConfig{}),
				Discover: func() []string { return []string{"10.0.0.1"} },
				NewInstance: func(address string) PostgresInstance {
					return NewMockInstance(address, nil, nil, func(string) bool { return healthy })
				},
				Initialize: true,
				RunSetupWithRetry: func(setup func() error) {
					for range 2 {
						attempts++
						err := setup()
						assert.Equal(t, attempts, reports, "report every attempt before returning to the retry adapter")
						if healthy {
							assert.NoError(t, err)
							return
						}
						assert.EqualError(t, err, "no valid ips found")
					}
				},
				SetupFinished: func(time.Duration) { reports++ },
			})
			t.Cleanup(pool.ClosePools)
			assert.Zero(t, reports, "initial construction does not report periodic setup latency")

			pool.SetupWithRetry()
			if healthy {
				assert.Equal(t, 1, reports)
			} else {
				assert.Equal(t, 2, reports)
			}
		})
	}
}

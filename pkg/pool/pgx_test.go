package pool

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicaPGXConfigPreservesConnectionFields(t *testing.T) {
	cfg, err := ReplicaPGXConfig("host=db.example.test port=5433 user=myuser dbname=mydb sslmode=require")
	require.NoError(t, err)
	assert.Equal(t, "db.example.test", cfg.ConnConfig.Host)
	assert.Equal(t, uint16(5433), cfg.ConnConfig.Port)
	assert.Equal(t, "myuser", cfg.ConnConfig.User)
	assert.Equal(t, "mydb", cfg.ConnConfig.Database)
	assert.Equal(t, ReplicaMaxConnLifetime, cfg.MaxConnLifetime)
	assert.Equal(t, ReplicaMaxConnLifetimeJitter, cfg.MaxConnLifetimeJitter)
}

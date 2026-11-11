// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

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

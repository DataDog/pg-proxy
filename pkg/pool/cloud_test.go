// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func TestCloudPoolInitializesStaticInstances(t *testing.T) {
	direct := NewDirectRuntime(DirectRuntimeConfig{DatabaseHost: "postgres.test"})
	cloud := NewCloudPool(CloudPoolConfig{
		Direct: direct,
		Instances: map[string]PostgresInstance{
			"10.0.0.1": NewMockInstance("10.0.0.1", &pgxpool.Pool{}, nil, func(string) bool { return true }),
			"10.0.0.2": NewMockInstance("10.0.0.2", &pgxpool.Pool{}, nil, func(string) bool { return false }),
		},
	})

	assert.ElementsMatch(t, []string{"10.0.0.1"}, cloud.State().HealthyAddresses())
	_, found := cloud.State().Load("10.0.0.2")
	assert.True(t, found)
}

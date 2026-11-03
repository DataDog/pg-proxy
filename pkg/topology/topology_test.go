package topology

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDynamicRoutingEnabled(t *testing.T) {
	tests := []struct {
		name             string
		clientConfigured bool
		backendSupports  bool
		logicalDatabase  LogicalDatabase
		expected         bool
	}{
		{
			name:            "has read clusters",
			logicalDatabase: LogicalDatabase{ReadClusters: ReadClusters{"read-cluster": nil}},
			expected:        true,
		},
		{
			name:             "both read clusters and client dynamic routing configured",
			clientConfigured: true,
			logicalDatabase:  LogicalDatabase{ReadClusters: ReadClusters{"read-cluster": nil}},
			expected:         true,
		},
		{
			name:             "dynamic routing configured with replicas",
			clientConfigured: true,
			backendSupports:  true,
			expected:         true,
		},
		{
			name:             "dynamic routing configured without replicas",
			clientConfigured: true,
			expected:         false,
		},
		{
			name:     "both read clusters and client dynamic routing NOT configured",
			expected: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, DynamicRoutingEnabled(test.clientConfigured, test.backendSupports, test.logicalDatabase))
		})
	}
}

func TestReadClustersChanged(t *testing.T) {
	tests := []struct {
		name     string
		old      ReadClusters
		new      ReadClusters
		expected bool
	}{
		{
			name:     "both nil",
			old:      nil,
			new:      nil,
			expected: false,
		},
		{
			name: "identical clusters and weights",
			old:  ReadClusters{"cluster1": {Weight: 50}},
			new:  ReadClusters{"cluster1": {Weight: 50}},
			// note: different pointer, same value — must be equal
			expected: false,
		},
		{
			name:     "read clusters added where none existed",
			old:      nil,
			new:      ReadClusters{"cluster1": {Weight: 50}},
			expected: true,
		},
		{
			name:     "read clusters removed",
			old:      ReadClusters{"cluster1": {Weight: 50}},
			new:      nil,
			expected: true,
		},
		{
			name:     "cluster weight changed",
			old:      ReadClusters{"cluster1": {Weight: 50}},
			new:      ReadClusters{"cluster1": {Weight: 70}},
			expected: true,
		},
		{
			name: "cluster added",
			old:  ReadClusters{"cluster1": {Weight: 50}},
			new: ReadClusters{
				"cluster1": {Weight: 50},
				"cluster2": {Weight: 80},
			},
			expected: true,
		},
		{
			name: "cluster removed",
			old: ReadClusters{
				"cluster1": {Weight: 50},
				"cluster2": {Weight: 80},
			},
			new:      ReadClusters{"cluster1": {Weight: 50}},
			expected: true,
		},
		{
			name:     "cluster replaced with different cluster",
			old:      ReadClusters{"cluster1": {Weight: 50}},
			new:      ReadClusters{"cluster2": {Weight: 50}},
			expected: true,
		},
		{
			name:     "nil setting unchanged",
			old:      ReadClusters{"cluster1": nil},
			new:      ReadClusters{"cluster1": nil},
			expected: false,
		},
		{
			name:     "nil setting replaced",
			old:      ReadClusters{"cluster1": nil},
			new:      ReadClusters{"cluster1": {Weight: DefaultReadClusterWeight}},
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, ReadClustersChanged(test.old, test.new))
		})
	}
}

func TestWeightHelpers(t *testing.T) {
	zero := uint8(0)
	assert.Equal(t, DefaultReadClusterWeight, NormalizedWeight(nil))
	assert.Equal(t, uint8(0), NormalizedWeight(&zero))
	assert.Equal(t, uint8(0), Weight(&ReadCluster{Weight: 0}))
	assert.Equal(t, 3, TotalWeight(ReadClusters{
		"zero":  {Weight: 0},
		"three": {Weight: 3},
	}))
}

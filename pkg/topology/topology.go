// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package topology

const (
	// DefaultReadClusterWeight is used when a read cluster weight is unset.
	DefaultReadClusterWeight uint8 = 1

	// DefaultPostgresMessageBufferSize is the default frontend/backend message
	// buffer size used by a session.
	DefaultPostgresMessageBufferSize = 10_240
)

// ReadCluster is the normalized runtime routing configuration for a read
// cluster. Weight is a value because config-specific unset handling belongs at
// the application's conversion boundary.
type ReadCluster struct {
	Weight uint8
}

// ReadClusters maps cluster names to their runtime routing configuration.
//
// Values remain pointers so a nil setting remains distinguishable from a
// present setting. Helpers compare pointed-to values, not pointer addresses.
type ReadClusters map[string]*ReadCluster

// LogicalDatabase is the runtime routing view of a logical database.
type LogicalDatabase struct {
	Cluster      string
	ReadClusters ReadClusters
}

// RuntimeConfig contains portable process configuration used by the listener
// runtime.
type RuntimeConfig struct {
	PostgresMessageBufferSize int
}

// NormalizedWeight returns weight, applying the default for an unset value.
func NormalizedWeight(weight *uint8) uint8 {
	if weight == nil {
		return DefaultReadClusterWeight
	}
	return *weight
}

// Weight returns a read cluster's normalized weight.
func Weight(readCluster *ReadCluster) uint8 {
	// TODO: Prevent a panic when readCluster is nil. The app usually removes nil
	// values when it loads the config, but library callers can still pass nil.
	return readCluster.Weight
}

// TotalWeight returns the sum of all normalized read cluster weights.
func TotalWeight(readClusters ReadClusters) int {
	var total int
	for _, readCluster := range readClusters {
		total += int(Weight(readCluster))
	}
	return total
}

// DynamicRoutingEnabled resolves client-requested and configured read routing.
func DynamicRoutingEnabled(clientConfigured, backendSupportsDynamicRouting bool, logicalDatabase LogicalDatabase) bool {
	if len(logicalDatabase.ReadClusters) > 0 {
		return true
	}
	if clientConfigured && !backendSupportsDynamicRouting {
		return false
	}
	return clientConfigured
}

// ReadClustersChanged returns true if the readClusters config has meaningfully changed.
func ReadClustersChanged(old, new ReadClusters) bool {
	if len(old) != len(new) {
		return true
	}
	for name, newSettings := range new {
		oldSettings, ok := old[name]
		if !ok {
			return true
		}
		if newSettings == nil || oldSettings == nil {
			if newSettings != oldSettings {
				return true
			}
			continue
		}
		if *newSettings != *oldSettings {
			return true
		}
	}
	return false
}

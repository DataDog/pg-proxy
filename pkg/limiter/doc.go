// Package limiter tracks global and per-service session counts.
//
// ReadHardLimit discovers the process cgroup hard memory cap: statfs to
// detect v1 vs v2, then memory.max or memory.limit_in_bytes. User-config
// mapping and feature-flag gating stay with the embedder.
package limiter

// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package limiter tracks global and per-service session counts.
//
// ReadHardLimit discovers the process cgroup hard memory cap: statfs to
// detect v1 vs v2, then memory.max or memory.limit_in_bytes. User-config
// mapping stays with the embedder.
package limiter

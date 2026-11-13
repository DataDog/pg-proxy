// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package health tracks proxy liveness, readiness, and per-cluster backend
// checks in one place.
//
// State is the single source for every health surface. The HTTP admin
// listener reads Snapshot for /livez and /readyz, and a SQL admin command can
// read the same Snapshot to show per-cluster detail. Surfaces choose how much
// to show: unauthenticated HTTP reports only the status word, while an
// authenticated surface can show cluster names, stages, and latency.
//
// Checks open their own short-lived connections. They never borrow from or
// change serving pools, and results never keep driver errors, which can
// contain hosts or users.
package health

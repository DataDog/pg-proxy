// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package sessionsql scans SQL text for what it may do to the session state
// that pg-proxy restores when it returns a backend connection to the pool.
//
// Scan answers "may have changed", never "did change": a wrong "may change"
// costs one reset, while a wrong "cannot change" leaks a setting to the next
// client. It fails closed on anything it cannot lex. It does not see changes
// made inside functions or triggers that the calling SQL does not name.
//
// The scanner is pure Go and makes no allocations. Its fuzz test compares it
// with the pg_query parse tree that pkg/classifier already links.
package sessionsql

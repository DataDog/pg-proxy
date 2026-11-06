// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package query decides what to do with query-execution messages once a
// connection is authenticated: what a message from Postgres means for a
// session (DecidePgMessageAction), how to size messages for buffering, and
// how to trace that message flow (TraceState).
package query

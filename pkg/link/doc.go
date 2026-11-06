// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package link associates a short-lived Session with a pooled Postgres
// connection: expectation bookkeeping for the wire protocol, prepared-statement
// metadata on the conn, and acquire/release lifecycle (including optional role
// passthrough via RoleSession). Routing and pool acquire stay with the embedder;
// New wraps an already-acquired *pgxpool.Conn.
package link

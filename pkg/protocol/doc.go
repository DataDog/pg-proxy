// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package protocol implements PostgreSQL wire-protocol conversations. It covers
// the cleartext password exchange; SCRAM-SHA-256 is in pkg/scram.
//
// Conversations are driven through an Exchange rather than a connection, so one
// can be tested by scripting messages instead of opening a connection. Exchange
// and its connection-backed implementation live here because mechanisms in other
// packages use them too.
package protocol

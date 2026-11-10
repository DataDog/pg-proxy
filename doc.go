// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package pgproxy is a PostgreSQL wire-protocol proxy with transaction-level
// connection pooling.
//
// Runtime integrations — configuration, feature flags, authentication, and load
// shedding — are supplied by the embedder through Go interfaces, so the proxy
// can run standalone or inside a larger system.
//
// The API is not yet stable.
package pgproxy

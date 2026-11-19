// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package bindparams copies Bind parameter values and renders them as decoded,
// truncated, per-position trace tags.
//
// Capture.Record runs on the Bind path of sampled traces only. It copies a
// bounded prefix of each value into buffers the Capture reuses, because
// pgproto3 reuses its read buffers. Capture.Render runs when the root trace
// finishes and emits db.parameters.0 … db.parameters.n, plus
// pgproxy.bind.truncated and pgproxy.bind.rendered when a limit drops values.
//
// NULL renders as null and a text value as its first ValueLimit bytes. Binary
// values are decoded from the type OID: int, float, bool, uuid, date,
// timestamp, numeric and one-dimensional or multi-dimensional arrays of them.
// Binary values of unknown type, and malformed ones, fall back to a \x hex
// prefix. Decoding reads
// untrusted bytes, so every decoder checks lengths and bounds its work by the
// output it may emit. Output is always valid UTF-8.
package bindparams

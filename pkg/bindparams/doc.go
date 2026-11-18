// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package bindparams copies Bind parameter values and renders them as typed,
// truncated, per-position trace tags.
//
// Capture.Record runs on the Bind path of sampled traces only. It copies a
// bounded prefix of each value into buffers the Capture reuses, because
// pgproto3 reuses its read buffers. Capture.Render runs when the root trace
// finishes and emits db.parameters.0 … db.parameters.n, plus
// db.parameters.truncated when a limit drops values.
//
// Values are decoded from the type OID: NULL as null, text by its first
// ValueLimit bytes, and binary int, float, bool, uuid, date, timestamp,
// numeric and one-dimensional or multi-dimensional arrays of them. Unknown or
// malformed binary values fall back to a \x hex prefix. Decoding reads
// untrusted bytes, so every decoder checks lengths and bounds its work by the
// output it may emit. Output is always valid UTF-8.
package bindparams

// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package prepared tracks client↔proxy prepared-statement name mappings for
// transaction-level pooling: pending Parse messages, confirmed named
// statements, and the auto-cache of unnamed statements onto named proxy names.
package prepared

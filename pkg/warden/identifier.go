// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package warden

// Identifier identifies a resource uniquely among resources of the same kind.
// For example, a database uses its name, while a user may use "<email>|<group>".
type Identifier string

// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The reply buffer pool is off unless the configuration file turns it on.
func TestReplyBufferPoolSetting(t *testing.T) {
	shared := ossConfig()
	cfg, err := (config{}).withSharedConfig(shared)
	require.NoError(t, err)
	require.False(t, cfg.replyBufferPool, "the reply buffer pool must be off by default")

	shared.Proxy.ReplyBufferPool = true
	cfg, err = (config{}).withSharedConfig(shared)
	require.NoError(t, err)
	require.True(t, cfg.replyBufferPool)
}

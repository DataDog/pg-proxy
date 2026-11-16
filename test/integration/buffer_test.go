// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package integration

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/pg-proxy/internal/itest"
)

// bufferSizes are the proxy message buffer sizes TestBufferBoundaries and
// TestPreparedIsolation use: a tiny one that most messages overflow, and a
// typical one.
var bufferSizes = []int{64, 4096}

// startProxyWithBuffer starts a proxy like startProxy with a message buffer of size bytes.
func startProxyWithBuffer(t *testing.T, size int) *itest.Proxy {
	t.Helper()
	cfg := proxyConfig()
	cfg.Proxy.PostgresMsgBufferSize = int32(size)
	return itest.MustStartProxy(t, cfg)
}

// TestBufferBoundaries verifies that the proxy relays a DataRow one byte
// smaller than, equal to and one byte larger than its message buffer, and
// that the session stays usable. Each case runs against PostgreSQL directly
// and through the proxy.
func TestBufferBoundaries(t *testing.T) {
	for _, size := range bufferSizes {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			var cases []protocolCase
			for _, delta := range []int{-1, 0, 1} {
				// The type byte, length, column count and field length take 11 bytes.
				payload := size + delta - 11
				cases = append(cases, protocolCase{
					name: fmt.Sprintf("data row of %d bytes", size+delta),
					steps: []step{
						ready(&pgproto3.Query{String: fmt.Sprintf("SELECT repeat('x', %d)", payload)}),
						ready(&pgproto3.Query{String: "SELECT 1"}),
					},
					check: func(t *testing.T, replies [][]pgproto3.BackendMessage) {
						row, ok := replies[0][1].(*pgproto3.DataRow)
						require.True(t, ok, "%v", summarize(replies[0]))
						encoded, err := row.Encode(nil)
						require.NoError(t, err)
						require.Len(t, encoded, size+delta)
						require.Equal(t, strings.Repeat("x", payload), dataRowValue(t, replies[0]))
						require.Equal(t, "1", dataRowValue(t, replies[1]))
					},
				})
			}
			runProtocolCases(t, startProxyWithBuffer(t, size), cases)
		})
	}
}

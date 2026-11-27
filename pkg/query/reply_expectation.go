// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package query

// A ReplyExpectation is an internal pg-proxy concept to keep track of replies that are expected from
// the Postgres connection, based on the client messages the SessionLink has proxied. A SessionLink
// maintains a FIFO queue of reply expectations that must be completely resolved on each flush event.
type ReplyExpectation int64

const (
	NoReply ReplyExpectation = iota // NoReply is a special value to indicate no expectations.
	QueryReply
	ParseReply
	BindReply
	DescribeReply
	ExecuteReply
	CloseReply
	SyncReply
	// ProxyCloseReply expects the CloseComplete for a Close the proxy sent on its
	// own. It is not forwarded to the client.
	ProxyCloseReply
)

func (e ReplyExpectation) String() string {
	switch e {
	case NoReply:
		return "NoReply"
	case QueryReply:
		return "QueryReply"
	case ParseReply:
		return "ParseReply"
	case BindReply:
		return "BindReply"
	case DescribeReply:
		return "DescribeReply"
	case ExecuteReply:
		return "ExecuteReply"
	case CloseReply:
		return "CloseReply"
	case SyncReply:
		return "SyncReply"
	case ProxyCloseReply:
		return "ProxyCloseReply"
	default:
		return "UnknownExpectation"
	}
}

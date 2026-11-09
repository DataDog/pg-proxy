// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package query

import "fmt"

// TxStatus is an enum representation of the statuses that may be returned
// by Postgres in a ReadyForQuery reply.
type TxStatus int

const (
	NoTxStatus TxStatus = iota // NoTxStatus is a special value
	Idle
	InTransaction
	InErroredTransaction
)

func TxStatusFromByte(b byte) (TxStatus, error) {
	switch b {
	case 'I':
		return Idle, nil
	case 'T':
		return InTransaction, nil
	case 'E':
		return InErroredTransaction, nil
	default:
		return NoTxStatus, fmt.Errorf("unexpected TxStatus: %c", b)
	}
}

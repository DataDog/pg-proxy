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

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
	default:
		return "UnknownExpectation"
	}
}

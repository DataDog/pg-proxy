package session

import (
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/DataDog/pg-proxy/pkg/query"
)

func (s *Session) recordClientMessageBodyLength(msg pgproto3.FrontendMessage) {
	if size, ok := clientMessageBodyLength(msg); ok {
		s.metrics.Distribution(MetricClientMessageBodyLength, float64(size),
			s.obsVals.messageMetricTags(query.MessageToText(msg)))
	}
}

// clientMessageBodyLength calculates the encoded body size from decoded fields,
// excluding the five-byte header. Do not call Encode here: that would copy the
// payload we are measuring. Authentication/startup messages are outside sessions.
func clientMessageBodyLength(msg pgproto3.FrontendMessage) (int, bool) {
	switch msg := msg.(type) {
	case *pgproto3.Bind:
		// Two string terminators, three uint16 counts, and one int32 length per
		// parameter (including NULL parameters, which have no value bytes).
		size := len(msg.DestinationPortal) + len(msg.PreparedStatement) + 8 +
			2*len(msg.ParameterFormatCodes) + 4*len(msg.Parameters) + 2*len(msg.ResultFormatCodes)
		for _, parameter := range msg.Parameters {
			size += len(parameter)
		}
		return size, true
	case *pgproto3.Query:
		return len(msg.String) + 1, true
	case *pgproto3.Parse:
		return len(msg.Name) + len(msg.Query) + 4 + 4*len(msg.ParameterOIDs), true
	case *pgproto3.CopyData:
		return len(msg.Data), true
	case *pgproto3.CopyFail:
		return len(msg.Message) + 1, true
	case *pgproto3.Close:
		return len(msg.Name) + 2, true
	case *pgproto3.Describe:
		return len(msg.Name) + 2, true
	case *pgproto3.Execute:
		return len(msg.Portal) + 5, true
	case *pgproto3.FunctionCall:
		// Function OID, two uint16 counts, result format, and argument lengths.
		size := 10 + 2*len(msg.ArgFormatCodes) + 4*len(msg.Arguments)
		for _, argument := range msg.Arguments {
			size += len(argument)
		}
		return size, true
	case *pgproto3.CopyDone, *pgproto3.Flush, *pgproto3.Sync, *pgproto3.Terminate:
		return 0, true
	default:
		return 0, false
	}
}

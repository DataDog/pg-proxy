package query

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
)

func TestGetMessageSize(t *testing.T) {
	assert := assert.New(t)
	testCases := []pgproto3.BackendMessage{
		&pgproto3.ParseComplete{},
		&pgproto3.BindComplete{},
		&pgproto3.CloseComplete{},
		&pgproto3.CommandComplete{},
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1000000")},
		&pgproto3.EmptyQueryResponse{},
		&pgproto3.RowDescription{},
		getRowDescription(),
		&pgproto3.NoData{},
		&pgproto3.ParameterDescription{},
		&pgproto3.ParameterDescription{ParameterOIDs: []uint32{1, 2, 3, 4, 5, 6}},
		&pgproto3.PortalSuspended{},
		&pgproto3.DataRow{},
		getDataRow(),
		&pgproto3.ReadyForQuery{},
		&pgproto3.ReadyForQuery{TxStatus: 'a'},
		&pgproto3.NoticeResponse{},
		&pgproto3.NoticeResponse{Severity: "SEVERE", Line: 123456, Detail: "DETAIL", UnknownFields: make(map[byte]string, 1000)},
		&pgproto3.ParameterStatus{},
		&pgproto3.ParameterStatus{Name: "Parameter", Value: "value"},
		&pgproto3.ErrorResponse{},
		&pgproto3.ErrorResponse{Severity: "SEVERE", Line: 123456, Detail: "DETAIL", Position: 1, Message: "MESSAGE"},
		&pgproto3.CopyData{},
		&pgproto3.CopyData{Data: []byte{1, 2, 3, 'a', 5, 6}},
		&pgproto3.CopyDone{},
		&pgproto3.CopyInResponse{},
		&pgproto3.CopyInResponse{OverallFormat: 'a', ColumnFormatCodes: []uint16{1, 2, 3, 4, 5, 6}},
		&pgproto3.CopyOutResponse{},
		&pgproto3.CopyOutResponse{OverallFormat: 'a', ColumnFormatCodes: []uint16{1, 2, 3, 4, 5, 6}},
	}
	for _, testCase := range testCases {
		size := -1
		switch m := testCase.(type) {
		case *pgproto3.ParseComplete:
			size = getHeaderMessageByteSize()
		case *pgproto3.BindComplete:
			size = getHeaderMessageByteSize()
		case *pgproto3.CloseComplete:
			size = getHeaderMessageByteSize()
		case *pgproto3.CommandComplete:
			size = getCommandCompleteByteSize(m)
		case *pgproto3.EmptyQueryResponse:
			size = getHeaderMessageByteSize()
		case *pgproto3.RowDescription:
			size = getRowDescriptionByteSize(m)
		case *pgproto3.NoData:
			size = getHeaderMessageByteSize()
		case *pgproto3.ParameterDescription:
			size = getParameterDescriptionByteSize(m)
		case *pgproto3.PortalSuspended:
			size = getHeaderMessageByteSize()
		case *pgproto3.DataRow:
			size = getDataRowByteSize(m)
		case *pgproto3.ReadyForQuery:
			size = getReadyForQueryByteSize(m)
		case *pgproto3.NoticeResponse:
			size = getNoticeResponseByteSize(m)
		case *pgproto3.ParameterStatus:
			size = getParameterStatusByteSize(m)
		case *pgproto3.ErrorResponse:
			size = getErrorResponseByteSize(m)
		case *pgproto3.CopyData:
			size = getCopyDataByteSize(m)
		case *pgproto3.CopyDone:
			size = getHeaderMessageByteSize()
		case *pgproto3.CopyInResponse:
			size = getCopyInResponseByteSize(m)
		case *pgproto3.CopyOutResponse:
			size = getCopyOutResponseByteSize(m)
		default:
			assert.Fail("test received unexpected message")
		}
		assert.Equalf(getMessageSize(testCase), size, fmt.Sprintf("unexpected size difference for %T", testCase))
	}
}

func BenchmarkGetRowDescriptionByteSize(b *testing.B) {
	m := getRowDescription()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		getRowDescriptionByteSize(m)
	}
}

func BenchmarkGetDataRowByteSize(b *testing.B) {
	m := getDataRow()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		getDataRowByteSize(m)
	}
}

func BenchmarkGetNoticeResponseByteSize(b *testing.B) {
	m := pgproto3.ErrorResponse{Severity: "FATAL", Message: "testing"}
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		getNoticeResponseByteSize((*pgproto3.NoticeResponse)(&m))
	}
}

func BenchmarkGetErrorResponseByteSize(b *testing.B) {
	m := pgproto3.ErrorResponse{Severity: "FATAL", Message: "testing"}
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		getErrorResponseByteSize(&m)
	}
}

// this gets the exact message size by first encoding it into a byte array and then looking at its size
// this utilizes the code that pgx uses to send messages, so we know it's the exact message size in the message buffer
func getMessageSize(m pgproto3.BackendMessage) int {
	b := make([]byte, 0)
	b, _ = m.Encode(b)
	return len(b)
}

func getRowDescription() *pgproto3.RowDescription {
	fieldDescriptions := []pgproto3.FieldDescription{
		pgproto3.FieldDescription{
			Name:                 []byte("Unknown"),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          213,
			DataTypeSize:         32,
			TypeModifier:         123,
			Format:               0,
		},
		pgproto3.FieldDescription{
			Name:                 []byte("More Unknown!"),
			TableOID:             10,
			TableAttributeNumber: 20,
			DataTypeOID:          3213,
			DataTypeSize:         432,
			TypeModifier:         5123,
			Format:               60,
		},
	}
	return &pgproto3.RowDescription{Fields: fieldDescriptions}
}

func getDataRow() *pgproto3.DataRow {
	rowCount := 1000
	values := make([][]byte, rowCount)
	row := make([]byte, rowCount)
	for range 1000 {
		values = append(values, row)
	}
	return &pgproto3.DataRow{Values: values}
}

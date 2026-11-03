package query

import (
	"github.com/jackc/pgx/v5/pgproto3"
	"strconv"
)

// Messages formats are documented here: https://www.postgresql.org/docs/current/protocol-message-formats.html
// In addition, look at how pgproto3.BackendMessage Encode functions are implemented to calculate the exact size
// of messages in the buffer
// We do not use the encode function to measure the message size it is much slower

// Most messages have the same format and share the same predetermined size
// 5 bytes = 1 byte identifier + 4 byte message length
func getHeaderMessageByteSize() int {
	return 5
}

// size = 1 byte identifier + 4 byte message length + 1 byte format + 2 byte column count + [column count] * 2 byte format code
func getCopyOutResponseByteSize(m *pgproto3.CopyOutResponse) int {
	return 8 + len(m.ColumnFormatCodes)*2
}

// size = 1 byte identifier + 4 byte message length + 1 byte format + 2 byte column count + [column count] * 2 byte format code
func getCopyInResponseByteSize(m *pgproto3.CopyInResponse) int {
	return 8 + len(m.ColumnFormatCodes)*2
}

// size = 1 byte identifier + 4 byte message length + n byte data
func getCopyDataByteSize(m *pgproto3.CopyData) int {
	return 5 + len(m.Data)
}

// size = 1 byte identifier + 4 byte message length + 1 byte pad + String parameter field + 1 byte pad + String parameter value
func getParameterStatusByteSize(m *pgproto3.ParameterStatus) int {
	return 7 + len(m.Name) + len(m.Value)
}

// 6 bytes = 1 byte identifier + 4 byte message length + 1 byte transaction status
func getReadyForQueryByteSize(m *pgproto3.ReadyForQuery) int {
	return 6
}

// size = 1 byte identifier + 4 byte message length + 2 byte parameter count + [parameter count] * (4 byte object id)
func getParameterDescriptionByteSize(m *pgproto3.ParameterDescription) int {
	return 7 + len(m.ParameterOIDs)*4
}

// 1 byte identifier + 4 byte message length + 1 byte pad + String command tag
func getCommandCompleteByteSize(m *pgproto3.CommandComplete) int {
	return 6 + len(m.CommandTag)
}

// 1 byte identifier + 4 byte message length + 2 byte field count +
// [field count] * (String field name + 1 byte padding + 4 byte column/table identifier + 2 byte column of table + 4 byte id + 2 byte data type size + 4 byte type modifier + 2 byte format code)
func getRowDescriptionByteSize(m *pgproto3.RowDescription) int {
	size := 7
	for _, f := range m.Fields {
		size += 19 + len(f.Name)
	}
	return size
}

// size = 1 byte identifier + 4 byte message length + 2 byte column value count + [column value count] * (4 byte column value length + n byte column values)
func getDataRowByteSize(m *pgproto3.DataRow) int {
	size := 7
	for _, c := range m.Values {
		size += 4 + len(c)
	}
	return size
}

// size = 1 byte identifier + 4 byte message length + [field count] * (1 byte field type + String field value)
func getNoticeResponseByteSize(m *pgproto3.NoticeResponse) int {
	return getErrorResponseByteSize((*pgproto3.ErrorResponse)(m))
}

// size = 1 byte identifier + 4 byte message length + [field count] * (1 byte field type + String field value)
// in order to avoid accessing each possible field to determine if it is present in the message, we encode it into
// a byte array and take the size of that
func getErrorResponseByteSize(m *pgproto3.ErrorResponse) int {
	size := 6 // all messages end with appended 0 byte

	// each message has 2 bytes for the field descriptor and for padding
	if m.Severity != "" {
		size += 2 + len(m.Severity)
	}
	if m.SeverityUnlocalized != "" {
		size += 2 + len(m.SeverityUnlocalized)
	}
	if m.Code != "" {
		size += 2 + len(m.Code)
	}
	if m.Message != "" {
		size += 2 + len(m.Message)
	}
	if m.Detail != "" {
		size += 2 + len(m.Detail)
	}
	if m.Hint != "" {
		size += 2 + len(m.Hint)
	}
	if m.Position != 0 {
		// at most int32 but could be smaller
		size += 2 + len([]byte(strconv.Itoa(int(m.Position))))
	}
	if m.InternalPosition != 0 {
		// at most int32 but could be smaller
		size += 2 + len([]byte(strconv.Itoa(int(m.InternalPosition))))
	}
	if m.InternalQuery != "" {
		size += 2 + len(m.InternalQuery)
	}
	if m.Where != "" {
		size += 2 + len(m.Where)
	}
	if m.SchemaName != "" {
		size += 2 + len(m.SchemaName)
	}
	if m.TableName != "" {
		size += 2 + len(m.TableName)
	}
	if m.ColumnName != "" {
		size += 2 + len(m.ColumnName)
	}
	if m.DataTypeName != "" {
		size += 2 + len(m.DataTypeName)
	}
	if m.ConstraintName != "" {
		size += 2 + len(m.ConstraintName)
	}
	if m.File != "" {
		size += 2 + len(m.File)
	}
	if m.Line != 0 {
		// at most int32 but could be smaller
		size += 2 + len([]byte(strconv.Itoa(int(m.Line))))
	}
	if m.Routine != "" {
		size += 2 + len(m.Routine)
	}

	for _, field := range m.UnknownFields {
		size += 2 + len(field)
	}

	return size
}

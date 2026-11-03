package otel

import (
	"net/url"
	"strings"

	"go.opentelemetry.io/otel/propagation"
)

// sqlComments reads the leading and trailing sqlcommenter blocks independently.
// It neither changes the SQL nor searches string literals or interior comments.
// Invalid/duplicate fields discard the carrier; tracing must never reject SQL.
func sqlComments(sql string) [2]propagation.MapCarrier {
	sql = strings.TrimSpace(sql)
	var comments [2]propagation.MapCarrier
	if strings.HasPrefix(sql, "/*") {
		if end := strings.Index(sql[2:], "*/"); end >= 0 {
			comments[0] = parseSQLComment(sql[2 : 2+end])
		}
	}
	if strings.HasSuffix(sql, "*/") {
		if start := strings.LastIndex(sql, "/*"); start >= 0 && start <= len(sql)-4 {
			comments[1] = parseSQLComment(sql[start+2 : len(sql)-2])
		}
	}
	return comments
}

func parseSQLComment(comment string) propagation.MapCarrier {
	if len(comment) > 8192 || strings.Contains(comment, "/*") || strings.Contains(comment, "*/") {
		return nil
	}
	carrier := propagation.MapCarrier{}
	for _, field := range strings.Split(comment, ",") {
		key, value, ok := strings.Cut(strings.TrimSpace(field), "=")
		value = strings.TrimSpace(value)
		if !ok || len(value) < 2 || value[0] != '\'' || value[len(value)-1] != '\'' {
			return nil
		}
		key, err := url.PathUnescape(strings.TrimSpace(key))
		if err != nil {
			return nil
		}
		value, err = url.PathUnescape(value[1 : len(value)-1])
		if err != nil {
			return nil
		}
		if _, exists := carrier[key]; exists {
			return nil
		}
		carrier[key] = value
	}
	return carrier
}

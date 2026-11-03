package otel

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/noop"
)

// NewLogHandler forwards slog records to a caller-owned OTel logger. The caller
// controls filtering, processing and export through the logger provider.
func NewLogHandler(logger otellog.Logger) slog.Handler {
	if logger == nil {
		logger = noop.NewLoggerProvider().Logger("pg-proxy")
	}
	return &logHandler{logger: logger}
}

type logHandler struct {
	logger otellog.Logger
	attrs  []attribute.KeyValue
	group  string
}

func severity(level slog.Level) otellog.Severity {
	return otellog.Severity(int(level) + int(otellog.SeverityInfo))
}
func (h *logHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.logger.Enabled(ctx, otellog.EnabledParameters{Severity: severity(level)})
}
func (h *logHandler) Handle(ctx context.Context, r slog.Record) error {
	var record otellog.Record
	record.SetTimestamp(r.Time)
	record.SetObservedTimestamp(time.Now())
	record.SetSeverity(severity(r.Level))
	record.SetSeverityText(r.Level.String())
	record.SetBody(attribute.StringValue(escapeInvalidUTF8(r.Message)))
	record.AddAttributes(h.attrs...)
	r.Attrs(func(a slog.Attr) bool { record.AddAttributes(logAttrs(h.group, a)...); return true })
	h.logger.Emit(ctx, record)
	return nil
}
func (h *logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := *h
	next.attrs = slices.Clone(h.attrs)
	for _, a := range attrs {
		next.attrs = append(next.attrs, logAttrs(h.group, a)...)
	}
	return &next
}
func (h *logHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	next := *h
	next.group += name + "."
	return &next
}
func logAttrs(prefix string, a slog.Attr) []attribute.KeyValue {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return nil
	}
	if a.Value.Kind() == slog.KindGroup {
		if a.Key != "" {
			prefix += a.Key + "."
		}
		var attrs []attribute.KeyValue
		for _, child := range a.Value.Group() {
			attrs = append(attrs, logAttrs(prefix, child)...)
		}
		return attrs
	}
	var value attribute.Value
	switch a.Value.Kind() {
	case slog.KindString:
		value = attribute.StringValue(escapeInvalidUTF8(a.Value.String()))
	case slog.KindBool:
		value = attribute.BoolValue(a.Value.Bool())
	case slog.KindInt64:
		value = attribute.Int64Value(a.Value.Int64())
	case slog.KindFloat64:
		value = attribute.Float64Value(a.Value.Float64())
	case slog.KindDuration:
		value = attribute.Int64Value(int64(a.Value.Duration()))
	case slog.KindTime:
		value = attribute.StringValue(a.Value.Time().Format(time.RFC3339Nano))
	default:
		value = attribute.StringValue(escapeInvalidUTF8(fmt.Sprint(a.Value.Any())))
	}
	return []attribute.KeyValue{{Key: attribute.Key(prefix + a.Key), Value: value}}
}

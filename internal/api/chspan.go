package api

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

// The name and tracer of the client span of one ClickHouse statement a query
// path sends, described by chQuerySpanOptions.
const (
	chQuerySpanName = "clickhouse.query"
	chTracerName    = "wavehouse-api"
)

// chQuerySpanOptions describes the client span of one ClickHouse statement a
// query path sends, operation as the query histogram names it, so a slow or
// failed call stands apart from the server span around it (#897). It names
// the statement by its verb alone: the text and parameters can hold row
// values. t names the server; withPort is false on the native protocol, whose
// port t's HTTP URL does not carry. Each call site starts and ends the span
// itself, setting its result with setCHQueryResult before the end.
func chQuerySpanOptions(t chconn.Target, withPort bool, operation, sql string, attrs ...attribute.KeyValue) []trace.SpanStartOption {
	attrs = append(attrs,
		semconv.DBSystemKey.String("clickhouse"),
		semconv.DBOperation(sqlVerb(sql)),
		attribute.String("operation", operation),
	)
	if t.Database != "" {
		attrs = append(attrs, semconv.DBName(t.Database))
	}
	if u, err := url.Parse(t.URL); err == nil && u.Hostname() != "" {
		attrs = append(attrs, semconv.ServerAddress(u.Hostname()))
		if port, err := strconv.Atoi(u.Port()); err == nil && withPort {
			attrs = append(attrs, semconv.ServerPort(port))
		}
	}
	return []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...)}
}

// setCHQueryResult sets result on span, the label the query histogram
// records the call under. A failure, but not a call its client walked away
// from, sets error status described by result alone: ClickHouse's message can
// quote the statement.
func setCHQueryResult(span trace.Span, result string) {
	span.SetAttributes(attribute.String("result", result))
	if result != resultOK && result != resultCanceled {
		span.SetAttributes(semconv.ErrorTypeKey.String(result))
		span.SetStatus(codes.Error, result)
	}
}

// sqlVerbs are the statement keywords a span names; any other leading word is
// OTHER, since on the raw-SQL path the word is the caller's and could be
// statement text.
var sqlVerbs = map[string]bool{
	"ALTER": true, "ATTACH": true, "BACKUP": true, "CHECK": true, "CREATE": true,
	"DELETE": true, "DESC": true, "DESCRIBE": true, "DETACH": true, "DROP": true,
	"EXCHANGE": true, "EXISTS": true, "EXPLAIN": true, "GRANT": true, "INSERT": true,
	"KILL": true, "OPTIMIZE": true, "RENAME": true, "RESTORE": true, "REVOKE": true,
	"SELECT": true, "SET": true, "SHOW": true, "SYSTEM": true, "TRUNCATE": true,
	"UNDROP": true, "UPDATE": true, "USE": true, "WATCH": true,
}

// sqlVerb is sql's leading keyword, upper-cased, read past comments and an
// `EXECUTE AS` prefix as IsMutation reads it, or OTHER when it is no
// statement keyword. A WITH-led statement is the SELECT or INSERT it runs.
func sqlVerb(sql string) string {
	s := stripLeadingSQLComments(sql)
	if rest, ok := skipExecuteAs(s); ok {
		s = rest
	}
	verb := strings.ToUpper(s[:skipWord(s, 0)])
	switch {
	case verb == "WITH" && IsMutation(s):
		return "INSERT"
	case verb == "WITH":
		return "SELECT"
	case sqlVerbs[verb]:
		return verb
	}
	return "OTHER"
}

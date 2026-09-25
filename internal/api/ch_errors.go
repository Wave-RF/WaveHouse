package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/chconn"
)

// The machine-readable codes a failed ClickHouse call answers with on the
// query paths (/v1/query, /v1/pipes/{name}, /v1/ops/query), in the error
// envelope's "code" field. They are API surface: rename none.
const (
	// codeCHRejected: ClickHouse judged the statement and refused it — bad
	// SQL, an unknown table or column, a type mismatch. 400.
	codeCHRejected = "clickhouse.rejected"
	// codeCHLimitExceeded: the query outran a limit it ran under — rows
	// read, result size, or the role's time or memory cap. 400.
	codeCHLimitExceeded = "clickhouse.limit_exceeded"
	// codeCHAccessDenied: ClickHouse's user lacks a grant the statement
	// needs (ACCESS_DENIED). 403.
	codeCHAccessDenied = "clickhouse.access_denied"
	// codeCHMisconfigured: ClickHouse refused the credentials or database
	// WaveHouse connects with — an operator fix, not a caller's. 502.
	codeCHMisconfigured = "clickhouse.misconfigured"
	// codeCHUnavailable: ClickHouse, or the way to it, could not take the
	// query now. 503 with Retry-After.
	codeCHUnavailable = "clickhouse.unavailable"
	// codeCHResponseTooLarge: the raw-SQL proxy's response cap. 502.
	codeCHResponseTooLarge = "clickhouse.response_too_large"
	// codeCHUnknown: a failure with no verdict. 5xx, retryable.
	codeCHUnknown = "clickhouse.unknown"
)

// retryAfterClickHouse is the Retry-After on a ClickHouse outage: long
// enough for a restart or a dropped connection to come back, short enough
// that a blip costs a caller seconds.
const retryAfterClickHouse = "5"

// ClickHouse exception codes the query paths read beyond chconn.Classify.
const (
	chTooManyRows       int32 = 158
	chTimeoutExceeded   int32 = 159
	chTooSlow           int32 = 160
	chMemoryLimit       int32 = 241
	chTooManyBytes      int32 = 307
	chTooManyRowsOrByte int32 = 396
	chAccessDenied      int32 = 497
)

// capBackstop is how long past a role's time cap the client waits for
// ClickHouse's own TIMEOUT_EXCEEDED before giving up on the query.
const capBackstop = 2 * time.Second

// cancelAfter is parent cancelled after d, with no deadline on it: the
// driver derives max_execution_time from a deadline, overriding the one
// the role's cap sends.
func cancelAfter(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	t := time.AfterFunc(d, cancel)
	return ctx, func() { t.Stop(); cancel() }
}

// queryCaps says which of the role's own resource caps a query ran under,
// so exceeding one reads as the query's cost rather than an outage.
type queryCaps struct {
	time, memory bool
}

// chFailure is how one failed ClickHouse call answers.
type chFailure struct {
	status    int
	code      string
	retryable bool
}

// chFailureOf maps a failed ClickHouse call onto the response, by the class
// chconn.Classify gives it. unknownStatus is the status of a failure with no
// verdict: 502 on the proxy, whose upstream answered with something it
// could not class, 500 on the native paths.
func chFailureOf(err error, unknownStatus int, caps queryCaps) chFailure {
	code, hasCode := chconn.ExceptionCode(err)
	switch {
	case hasCode && (code == chTooManyRows || code == chTooManyBytes || code == chTooManyRowsOrByte),
		caps.time && hasCode && (code == chTimeoutExceeded || code == chTooSlow),
		caps.memory && hasCode && code == chMemoryLimit:
		return chFailure{http.StatusBadRequest, codeCHLimitExceeded, false}
	}
	switch chconn.Classify(err) {
	case chconn.Rejected:
		return chFailure{http.StatusBadRequest, codeCHRejected, false}
	case chconn.Denied:
		// A missing grant refuses this statement; every other denial —
		// the password, the user, the database — refuses every query
		// WaveHouse sends, which only the operator can fix.
		if hasCode && code == chAccessDenied {
			return chFailure{http.StatusForbidden, codeCHAccessDenied, false}
		}
		return chFailure{http.StatusBadGateway, codeCHMisconfigured, false}
	case chconn.Unavailable:
		return chFailure{http.StatusServiceUnavailable, codeCHUnavailable, true}
	case chconn.Unknown:
	}
	return chFailure{unknownStatus, codeCHUnknown, true}
}

// writeCHError answers a failed ClickHouse call with message, at the status
// and code its class maps to. A denial is also logged: it is ClickHouse's
// configuration refusing WaveHouse, which an operator should hear about
// even when the caller only sees a 403.
func writeCHError(w http.ResponseWriter, r *http.Request, err error, message string, unknownStatus int, caps queryCaps) {
	f := chFailureOf(err, unknownStatus, caps)
	switch f.code {
	case codeCHUnavailable:
		w.Header().Set("Retry-After", retryAfterClickHouse)
	case codeCHAccessDenied, codeCHMisconfigured:
		exCode, _ := chconn.ExceptionCode(err)
		slog.WarnContext(r.Context(), "clickhouse refused WaveHouse's user",
			slog.String("code", f.code), slog.Int("exception_code", int(exCode)),
			slog.String("path", r.URL.Path), slog.String("error", err.Error()))
	}
	writeJSONErrorBody(w, f.status, errorBody{Error: message, Code: f.code, Retryable: &f.retryable})
}

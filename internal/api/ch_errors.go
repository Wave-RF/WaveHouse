package api

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"
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
	// WaveHouse connects with, or whatever sits in front of it answered a
	// redirect or a 4xx with no exception code — an operator fix, not a
	// caller's. 502.
	codeCHMisconfigured = "clickhouse.misconfigured"
	// codeCHUnavailable: ClickHouse, or the way to it, could not take the
	// query now. 503 with Retry-After — without it for a write pipe, which
	// may have run and is never retryable.
	codeCHUnavailable = "clickhouse.unavailable"
	// codeCHResponseTooLarge: the response outgrew the buffer every query
	// path caps it at. 502, not retryable.
	codeCHResponseTooLarge = "clickhouse.response_too_large"
	// codeCHUnknown: a failure with no verdict. 5xx, retryable unless a
	// write pipe's.
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
	chReadonly          int32 = 164
	chMemoryLimit       int32 = 241
	chTooManyBytes      int32 = 307
	chTooManyRowsOrByte int32 = 396
	chAccessDenied      int32 = 497
)

// capBackstop is how long past the max_execution_time a read sends the
// client waits for ClickHouse's own TIMEOUT_EXCEEDED before giving up on the
// query: the server checks its budget between blocks, so it overshoots a
// little, and its answer says which limit stopped the query where a dropped
// connection says nothing.
const capBackstop = 2 * time.Second

// queryCaps says what a query ran under: which of the role's own resource
// caps, so exceeding one reads as the query's cost rather than an outage,
// and whether it ran as a read under readonly=2.
type queryCaps struct {
	time, memory bool
	// readonly marks a read sent with readonly=2. ClickHouse refusing it
	// as READONLY means the statement writes — a pipe IsMutation reads as a
	// read — or the user's profile is readonly=1 and refuses the settings
	// every read sends: either way the configuration, not the caller, and
	// the same again on a retry.
	readonly bool
}

// chFailure is how one failed ClickHouse call answers.
type chFailure struct {
	status    int
	code      string
	retryable bool
}

// chFailureOf maps a failed ClickHouse call onto the response, by the class
// chconn.Classify gives it. unknownStatus is the status of a failure with no
// verdict: 502 on the proxy, which forwards whatever its upstream answered,
// 500 on the structured query and pipes.
func chFailureOf(err error, unknownStatus int, caps queryCaps) chFailure {
	if _, ok := errors.AsType[*chResponseTooLargeError](err); ok {
		return chFailure{http.StatusBadGateway, codeCHResponseTooLarge, false}
	}
	code, hasCode := chconn.ExceptionCode(err)
	if caps.readonly && hasCode && code == chReadonly {
		return chFailure{http.StatusBadGateway, codeCHMisconfigured, false}
	}
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
		// A codeless 3xx or 4xx is not ClickHouse's answer but that of
		// whatever sits on the way to it — a wrong path, a proxy's rule —
		// and it answers the same on a retry.
		if status, ok := chconn.HTTPStatus(err); ok && status >= 300 && status < 500 {
			return chFailure{http.StatusBadGateway, codeCHMisconfigured, false}
		}
	}
	return chFailure{unknownStatus, codeCHUnknown, true}
}

// writeCHError answers a failed ClickHouse call with message, at the status
// and code its class maps to. A denial or misconfiguration is also logged:
// it is WaveHouse's configuration being refused, which an operator should
// hear about even when the caller only sees a 403.
func writeCHError(w http.ResponseWriter, r *http.Request, err error, message string, unknownStatus int, caps queryCaps) {
	writeCHFailure(w, r, err, message, chFailureOf(err, unknownStatus, caps))
}

// writeCHWriteError answers a failed write pipe as writeCHError does, but
// never as retryable and with no Retry-After: the statement may have reached
// ClickHouse and run, so a client retrying would run it again.
func writeCHWriteError(w http.ResponseWriter, r *http.Request, err error, message string) {
	f := chFailureOf(err, http.StatusInternalServerError, queryCaps{})
	f.retryable = false
	writeCHFailure(w, r, err, message, f)
}

// chErrorMessage is what a failed ClickHouse call tells the caller:
// ClickHouse's own text, verbatim, for a refusal it answered, as the proxy
// forwards it; the error itself for anything else.
func chErrorMessage(err error) string {
	if he, ok := errors.AsType[*chconn.HTTPError](err); ok {
		if msg := strings.TrimSpace(he.Body); msg != "" {
			return msg
		}
	}
	return err.Error()
}

func writeCHFailure(w http.ResponseWriter, r *http.Request, err error, message string, f chFailure) {
	switch f.code {
	case codeCHUnavailable:
		if f.retryable {
			w.Header().Set("Retry-After", retryAfterClickHouse)
		}
	case codeCHAccessDenied, codeCHMisconfigured:
		exCode, _ := chconn.ExceptionCode(err)
		slog.WarnContext(r.Context(), "clickhouse refused WaveHouse's configuration",
			slog.String("code", f.code), slog.Int("exception_code", int(exCode)),
			slog.String("path", r.URL.Path), slog.String("error", err.Error()))
	}
	writeJSONErrorBody(w, f.status, errorBody{Error: message, Code: f.code, Retryable: &f.retryable})
}

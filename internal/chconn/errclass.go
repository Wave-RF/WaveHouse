package chconn

import (
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"syscall"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Class is what a failed ClickHouse request says about the request itself:
// whether sending it again, unchanged, can succeed. The ingest worker retries
// every class but Rejected and dead-letters only Rejected; the query handlers
// map the same classes onto HTTP statuses (api/ch_errors.go).
type Class int

const (
	// Unknown is a failure with no verdict: ClickHouse sent no exception
	// code, and the error is not one the way to ClickHouse is known to
	// produce. Nothing says the request was judged.
	Unknown Class = iota
	// Unavailable means ClickHouse, or the way to it, could not take the
	// request now — refused or dropped connections, timeouts, overload,
	// read-only tables, lost replicas or Keeper. The request was not judged;
	// the same request can succeed later.
	Unavailable
	// Denied means ClickHouse refused the credentials or grants the request
	// ran under: the connection's configuration, not the request's content.
	Denied
	// Rejected means ClickHouse judged the request and refused it — a value
	// it cannot parse, a type mismatch, a table or column it does not have,
	// bad SQL. Sent again unchanged, it fails the same way.
	Rejected
)

func (c Class) String() string {
	switch c {
	case Unavailable:
		return "unavailable"
	case Denied:
		return "denied"
	case Rejected:
		return "rejected"
	case Unknown:
	}
	return "unknown"
}

// unavailableCodes are the ClickHouse exception codes that describe the
// server's state rather than the request (names from errorCodeToName on
// 26.8). The permanently read-only table (774) is here too: its rows are
// not at fault, and an operator fixes it the way one fixes a grant.
var unavailableCodes = map[int32]struct{}{
	95:   {}, // CANNOT_READ_FROM_SOCKET
	96:   {}, // CANNOT_WRITE_TO_SOCKET
	159:  {}, // TIMEOUT_EXCEEDED
	160:  {}, // TOO_SLOW
	164:  {}, // READONLY
	173:  {}, // CANNOT_ALLOCATE_MEMORY
	201:  {}, // QUOTA_EXCEEDED
	202:  {}, // TOO_MANY_SIMULTANEOUS_QUERIES
	203:  {}, // NO_FREE_CONNECTION
	209:  {}, // SOCKET_TIMEOUT
	210:  {}, // NETWORK_ERROR
	225:  {}, // NO_ZOOKEEPER
	236:  {}, // ABORTED
	241:  {}, // MEMORY_LIMIT_EXCEEDED
	242:  {}, // TABLE_IS_READ_ONLY
	243:  {}, // NOT_ENOUGH_SPACE
	244:  {}, // UNEXPECTED_ZOOKEEPER_ERROR
	252:  {}, // TOO_MANY_PARTS
	254:  {}, // NO_ACTIVE_REPLICAS
	265:  {}, // NO_AVAILABLE_REPLICA
	279:  {}, // ALL_CONNECTION_TRIES_FAILED
	285:  {}, // TOO_FEW_LIVE_REPLICAS
	286:  {}, // UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE
	289:  {}, // REPLICA_IS_NOT_IN_QUORUM
	297:  {}, // SHARD_HAS_NO_CONNECTIONS
	319:  {}, // UNKNOWN_STATUS_OF_INSERT
	364:  {}, // RECEIVED_ERROR_TOO_MANY_REQUESTS
	369:  {}, // ALL_REPLICAS_ARE_STALE
	394:  {}, // QUERY_WAS_CANCELLED
	415:  {}, // ALL_REPLICAS_LOST
	425:  {}, // SYSTEM_ERROR
	439:  {}, // CANNOT_SCHEDULE_TASK
	499:  {}, // S3_ERROR
	574:  {}, // DISTRIBUTED_TOO_MANY_PENDING_BYTES
	692:  {}, // TOO_MANY_MUTATIONS
	700:  {}, // USER_SESSION_LIMIT_EXCEEDED
	735:  {}, // QUERY_WAS_CANCELLED_BY_CLIENT
	745:  {}, // SERVER_OVERLOADED
	749:  {}, // TCP_CONNECTION_LIMIT_REACHED
	762:  {}, // HTTP_CONNECTION_LIMIT_REACHED
	774:  {}, // TABLE_IS_PERMANENTLY_READ_ONLY
	776:  {}, // RESOURCE_LIMIT_EXCEEDED
	777:  {}, // MEMORY_RESERVATION_KILLED
	778:  {}, // MEMORY_RESERVATION_FAILED
	904:  {}, // TOO_MANY_UNAVAILABLE_SHARDS
	999:  {}, // KEEPER_EXCEPTION
	1000: {}, // POCO_EXCEPTION
}

// deniedCodes are the exception codes that refuse the identity a request ran
// under rather than the request.
var deniedCodes = map[int32]struct{}{
	192: {}, // UNKNOWN_USER
	193: {}, // WRONG_PASSWORD
	194: {}, // REQUIRED_PASSWORD
	195: {}, // IP_ADDRESS_NOT_ALLOWED
	291: {}, // DATABASE_ACCESS_DENIED
	497: {}, // ACCESS_DENIED
	516: {}, // AUTHENTICATION_FAILED
	720: {}, // USER_EXPIRED
}

// tableScopedCodes are the retried codes that usually describe one table
// rather than the server: a read-only table, one with too many parts or
// mutations, or a grant missing on it leaves every other table on the same
// pool writable. A user denied everywhere still recovers, one table at a time.
var tableScopedCodes = map[int32]struct{}{
	242: {}, // TABLE_IS_READ_ONLY
	252: {}, // TOO_MANY_PARTS
	497: {}, // ACCESS_DENIED
	692: {}, // TOO_MANY_MUTATIONS
	774: {}, // TABLE_IS_PERMANENTLY_READ_ONLY
}

// TableScoped reports whether err is a retried failure of the one table the
// request wrote to, not of the server or the identity — so a caller backing
// off can hold back that table alone.
func TableScoped(err error) bool {
	code, ok := ExceptionCode(err)
	if !ok {
		return false
	}
	_, scoped := tableScopedCodes[code]
	return scoped
}

// splittableCodes are the retried codes a batch can earn by its size alone,
// each of its rows inserting on its own: a batch spanning more partitions than
// max_partitions_per_insert_block gets TOO_MANY_PARTS, and a large one can
// pass the memory limit. ClickHouse's own Distributed async inserts split a
// batch on these codes (isSplittableErrorCode).
var splittableCodes = map[int32]struct{}{
	241: {}, // MEMORY_LIMIT_EXCEEDED
	252: {}, // TOO_MANY_PARTS
}

// Splittable reports whether err is a retried failure that a smaller request
// may avoid — so a caller holding a batch should split it before backing off.
func Splittable(err error) bool {
	code, ok := ExceptionCode(err)
	if !ok {
		return false
	}
	_, split := splittableCodes[code]
	return split
}

// ClassOfCode classes a ClickHouse exception code. A code on neither list is
// Rejected: an exception code means the server was up and read the request,
// and most of ClickHouse's several hundred codes are about what it read. The
// availability codes are the enumerated exception, not the other way round.
func ClassOfCode(code int32) Class {
	if _, ok := unavailableCodes[code]; ok {
		return Unavailable
	}
	if _, ok := deniedCodes[code]; ok {
		return Denied
	}
	return Rejected
}

// Classify classes a failed ClickHouse request, over the HTTP interface
// (*HTTPError) or the driver (*clickhouse.Exception, *clickhouse.HTTPError,
// its sentinels, and the network errors under them). A nil error is Unknown.
func Classify(err error) Class {
	if err == nil {
		return Unknown
	}
	if code, ok := ExceptionCode(err); ok {
		return ClassOfCode(code)
	}
	if status, ok := httpStatus(err); ok {
		return classOfStatus(status)
	}
	if transportFailure(err) {
		return Unavailable
	}
	return Unknown
}

// ExceptionCode is the ClickHouse exception code err carries, if any.
func ExceptionCode(err error) (int32, bool) {
	var ex *clickhouse.Exception
	if errors.As(err, &ex) && ex.Code > 0 {
		return ex.Code, true
	}
	var he *HTTPError
	if errors.As(err, &he) && he.Code > 0 {
		return he.Code, true
	}
	return 0, false
}

func httpStatus(err error) (int, bool) {
	var he *HTTPError
	if errors.As(err, &he) {
		return he.StatusCode, true
	}
	var dhe *clickhouse.HTTPError
	if errors.As(err, &dhe) {
		return dhe.StatusCode, true
	}
	return 0, false
}

// classOfStatus classes a non-2xx response that carries no exception code —
// ClickHouse always sends one with an exception, so this is usually a proxy
// or load balancer in front of it answering for it.
func classOfStatus(status int) Class {
	switch status {
	case http.StatusRequestTimeout, http.StatusTooManyRequests,
		http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return Unavailable
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusProxyAuthRequired:
		return Denied
	case http.StatusRequestEntityTooLarge:
		// The body's size is the request's: smaller requests can pass.
		return Rejected
	default:
		return Unknown
	}
}

// transportFailure reports an error on the way to ClickHouse — the request
// never reached a verdict.
func transportFailure(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH) ||
		errors.Is(err, clickhouse.ErrAcquireConnTimeout) || errors.Is(err, clickhouse.ErrConnectionClosed) ||
		errors.Is(err, sqldriver.ErrBadConn) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// HTTPError is a non-2xx answer from the ClickHouse HTTP interface. Its
// message keeps the body verbatim — the dead-letter headers carry it.
type HTTPError struct {
	StatusCode int
	// Code is the ClickHouse exception code, 0 when the response carried
	// none (usually a proxy's answer, not ClickHouse's).
	Code int32
	Body string
}

func (e *HTTPError) Error() string { return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Body) }

// maxErrorBody caps how much of an error response is kept.
const maxErrorBody = 4096

var bodyCodeRe = regexp.MustCompile(`^Code:\s*(\d+)\.`)

// NewHTTPError reads a non-2xx ClickHouse HTTP response into an HTTPError,
// taking the exception code from the X-ClickHouse-Exception-Code header, or
// failing that from the body's "Code: NNN." prefix. The caller closes the
// body.
func NewHTTPError(resp *http.Response) *HTTPError {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	e := &HTTPError{StatusCode: resp.StatusCode, Body: string(body)}
	if c, err := strconv.ParseInt(resp.Header.Get("X-ClickHouse-Exception-Code"), 10, 32); err == nil && c > 0 {
		e.Code = int32(c)
	} else if m := bodyCodeRe.FindSubmatch(body); m != nil {
		if c, err := strconv.ParseInt(string(m[1]), 10, 32); err == nil && c > 0 {
			e.Code = int32(c)
		}
	}
	return e
}

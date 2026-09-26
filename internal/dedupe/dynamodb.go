package dedupe

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	mathrand "math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"

	"github.com/Wave-RF/WaveHouse/internal/tenant"
)

// The table's attributes. pk is the key from key.go and the only key
// attribute; ex is the table's TTL attribute.
const (
	attrKey    = "pk"
	attrState  = "st"
	attrExpiry = "ex"
	attrToken  = "tk"

	statePending   = "1"
	stateCommitted = "2"

	// A claim is live while now < ex; one whose ex has passed is absent
	// to Reserve, whether or not TTL has deleted it yet.
	condReserve = "attribute_not_exists(pk) OR ex <= :now"
	condRelease = "tk = :tk AND st = :pending"

	// batchWriteMax is BatchWriteItem's per-call item limit.
	batchWriteMax = 25
	// commitRounds bounds the BatchWriteItem rounds one chunk gets before
	// its still-unprocessed (or still-throttled) items fail the Commit.
	commitRounds = 8
	// commitBase and commitCeiling bound the jittered wait between Commit
	// rounds; retryBase is the one the SDK retryer's backoff doubles from.
	commitBase    = 25 * time.Millisecond
	commitCeiling = 200 * time.Millisecond
	retryBase     = 25 * time.Millisecond
	tokenBytes    = 16

	// opReserve is the operation the breaker watches: Release and Commit
	// answers say nothing about whether a new Reserve would get through.
	opReserve = "put_item"
)

// DynamoConfig is the DynamoDB backend's wiring. Credentials are never here:
// the SDK's default chain finds them (EKS Pod Identity or IRSA in a pod, the
// environment or a profile locally).
type DynamoConfig struct {
	// Table is the shared table every tenant's keys live in. Required.
	Table string
	// Region overrides the SDK chain's region (AWS_REGION) when set.
	Region string
	// Endpoint points the client at dynamodb-local. Tests and development
	// only; it is also what unlocks CreateTable.
	Endpoint string
	// Timeout bounds each DynamoDB call, its SDK retries included.
	// 0 = 250ms. The retries' jittered backoff is capped so that together
	// it waits at most half of Timeout (retryBackoff).
	Timeout time.Duration
	// MaxAttempts is the SDK retryer's attempts per call. 0 = 3.
	MaxAttempts int
	// RetryMode is "standard" (default) or "adaptive", which also rate-limits
	// the client after throttles.
	RetryMode string
	// ReserveConcurrency bounds the parallel calls one Reserve, Commit or
	// Release makes, and sizes the client's idle connection pool to match.
	// 0 = 64.
	ReserveConcurrency int
}

func (c DynamoConfig) withDefaults() DynamoConfig {
	if c.Timeout <= 0 {
		c.Timeout = 250 * time.Millisecond
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
	if c.RetryMode == "" {
		c.RetryMode = "standard"
	}
	if c.ReserveConcurrency <= 0 {
		c.ReserveConcurrency = 64
	}
	return c
}

// dynamoAPI is the part of *dynamodb.Client the backend calls, so a unit test
// can inject throttles and unprocessed items.
type dynamoAPI interface {
	PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	BatchWriteItem(context.Context, *dynamodb.BatchWriteItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	DescribeTable(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	DescribeTimeToLive(context.Context, *dynamodb.DescribeTimeToLiveInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error)
	CreateTable(context.Context, *dynamodb.CreateTableInput, ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	UpdateTimeToLive(context.Context, *dynamodb.UpdateTimeToLiveInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateTimeToLiveOutput, error)
}

// Dynamo is the DynamoDB implementation: every tenant's keys in one shared
// table, so pods sharing the table share seen ids and Reserve's conditional
// write is atomic across all of them. WaveHouse never creates the table in
// production; CreateTable is for dynamodb-local.
type Dynamo struct {
	api     dynamoAPI
	cfg     DynamoConfig
	now     func() time.Time
	breaker *breaker
	metrics dynamoMetrics
	// commitBackoff is the wait before retrying the attempt'th round of
	// unprocessed or throttled items.
	commitBackoff func(attempt int) time.Duration
}

// NewDynamo builds the backend over a client from the SDK's default config
// chain. extra is appended to the chain's options (a test's static
// credentials or HTTP client, say). It dials nothing: Check does.
func NewDynamo(ctx context.Context, cfg DynamoConfig, extra ...func(*config.LoadOptions) error) (*Dynamo, error) {
	if cfg.Table == "" {
		return nil, errors.New("dedupe: dynamodb table is required")
	}
	cfg = cfg.withDefaults()
	retryer, err := newRetryer(cfg)
	if err != nil {
		return nil, err
	}
	opts := []func(*config.LoadOptions) error{config.WithRetryer(retryer), config.WithHTTPClient(newHTTPClient(cfg))}
	if cfg.Region != "" {
		opts = append(opts, config.WithRegion(cfg.Region))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, append(opts, extra...)...)
	if err != nil {
		return nil, fmt.Errorf("dedupe: aws config: %w", err)
	}
	client := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})
	return newDynamo(client, cfg), nil
}

// newHTTPClient keeps an idle connection for every call one Reserve can have
// in flight: with the SDK's default of 10 per host, a wide Reserve would dial
// most of its puts afresh.
func newHTTPClient(cfg DynamoConfig) *awshttp.BuildableClient {
	return awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
		tr.MaxIdleConnsPerHost = cfg.ReserveConcurrency
		tr.MaxIdleConns = max(tr.MaxIdleConns, cfg.ReserveConcurrency)
	})
}

func newRetryer(cfg DynamoConfig) (func() aws.Retryer, error) {
	standard := func(o *retry.StandardOptions) {
		o.MaxAttempts = cfg.MaxAttempts
		o.Backoff = retryBackoff(cfg)
	}
	switch cfg.RetryMode {
	case "standard":
		return func() aws.Retryer { return retry.NewStandard(standard) }, nil
	case "adaptive":
		return func() aws.Retryer {
			return retry.NewAdaptiveMode(func(o *retry.AdaptiveModeOptions) {
				o.StandardOptions = append(o.StandardOptions, standard)
			})
		}, nil
	}
	return nil, fmt.Errorf("dedupe: dynamodb retry_mode %q: want standard or adaptive", cfg.RetryMode)
}

// retryBackoff is the SDK retryer's wait before a retry: full jitter, so
// puts throttled together do not retry in lockstep, under a ceiling that
// doubles from retryBase up to Timeout/(2·(MaxAttempts-1)). A call's retries
// then wait at most half its Timeout in all, so a throttled call ends on its
// last attempt's answer (ErrUnavailable, the throttle as its cause) unless
// the attempts themselves take the other half.
func retryBackoff(cfg DynamoConfig) retry.BackoffDelayerFunc {
	ceiling := cfg.Timeout / time.Duration(2*max(cfg.MaxAttempts-1, 1))
	return func(attempt int, _ error) (time.Duration, error) {
		return fullJitter(retryBase, ceiling, attempt), nil
	}
}

// fullJitter is uniform over [0, min(base·2^attempt, ceiling)].
func fullJitter(base, ceiling time.Duration, attempt int) time.Duration {
	d := min(base<<min(max(attempt, 0), 30), ceiling)
	if d <= 0 {
		return 0
	}
	return mathrand.N(d + 1) //nolint:gosec // G404: backoff jitter, not a secret
}

func newDynamo(api dynamoAPI, cfg DynamoConfig) *Dynamo {
	now := time.Now
	return &Dynamo{
		api:     api,
		cfg:     cfg.withDefaults(),
		now:     now,
		breaker: newBreaker(now),
		metrics: newDynamoMetrics(),
		commitBackoff: func(attempt int) time.Duration {
			return fullJitter(commitBase, commitCeiling, attempt)
		},
	}
}

// Tenant builds tenant id's store, closed, over its share of the table — the
// Factory Stores takes. The client is shared, so opening a store is free.
func (d *Dynamo) Tenant(id tenant.ID) *Managed {
	prefix := KeyPrefix(id)
	return NewManaged(func() (Deduplicator, error) { return &dynamoStore{d: d, prefix: prefix}, nil })
}

// Check verifies the table exists with the key schema this backend writes:
// pk, a string, as the only key. TTL not enabled on ex is logged, not refused:
// expiry never depends on it, only storage does.
func (d *Dynamo) Check(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*d.cfg.Timeout)
	defer cancel()
	out, err := d.api.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &d.cfg.Table})
	if err != nil {
		return classify("describe_table", err)
	}
	t := out.Table
	if len(t.KeySchema) != 1 || aws.ToString(t.KeySchema[0].AttributeName) != attrKey || t.KeySchema[0].KeyType != types.KeyTypeHash {
		return fmt.Errorf("dedupe: dynamodb table %s: key schema must be %s (HASH) alone", d.cfg.Table, attrKey)
	}
	for _, a := range t.AttributeDefinitions {
		if aws.ToString(a.AttributeName) == attrKey && a.AttributeType != types.ScalarAttributeTypeS {
			return fmt.Errorf("dedupe: dynamodb table %s: %s must be a string (S), is %s", d.cfg.Table, attrKey, a.AttributeType)
		}
	}
	ttl, err := d.api.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{TableName: &d.cfg.Table})
	if err != nil {
		return classify("describe_time_to_live", err)
	}
	if desc := ttl.TimeToLiveDescription; desc == nil || aws.ToString(desc.AttributeName) != attrExpiry ||
		(desc.TimeToLiveStatus != types.TimeToLiveStatusEnabled && desc.TimeToLiveStatus != types.TimeToLiveStatusEnabling) {
		slog.WarnContext(ctx, "dedupe: dynamodb table has no TTL on ex; expired ids are ignored but never deleted", "table", d.cfg.Table)
	}
	return nil
}

// ErrCreateTableNeedsEndpoint refuses CreateTable against real AWS: the
// production table belongs to the deployment's infrastructure code.
var ErrCreateTableNeedsEndpoint = errors.New("dedupe: create_table is for dynamodb-local only; set the endpoint")

// CreateTable creates the table on dynamodb-local, with TTL on ex, and waits
// for it. A table that already exists is left as it is.
func (d *Dynamo) CreateTable(ctx context.Context) error {
	if d.cfg.Endpoint == "" {
		return ErrCreateTableNeedsEndpoint
	}
	_, err := d.api.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            &d.cfg.Table,
		BillingMode:          types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String(attrKey), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String(attrKey), KeyType: types.KeyTypeHash}},
	})
	var inUse *types.ResourceInUseException
	if errors.As(err, &inUse) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("dedupe: create table %s: %w", d.cfg.Table, err)
	}
	if err := dynamodb.NewTableExistsWaiter(d.api).Wait(ctx, &dynamodb.DescribeTableInput{TableName: &d.cfg.Table}, time.Minute); err != nil {
		return fmt.Errorf("dedupe: wait for table %s: %w", d.cfg.Table, err)
	}
	_, err = d.api.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName:               &d.cfg.Table,
		TimeToLiveSpecification: &types.TimeToLiveSpecification{AttributeName: aws.String(attrExpiry), Enabled: aws.Bool(true)},
	})
	if err != nil {
		return fmt.Errorf("dedupe: enable ttl on %s: %w", d.cfg.Table, err)
	}
	return nil
}

// call runs one DynamoDB request under the per-call deadline, classifies its
// error, and records it for the metrics and the breaker.
func (d *Dynamo) call(ctx context.Context, op string, do func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, d.cfg.Timeout)
	defer cancel()
	start := time.Now()
	err := classify(op, do(ctx))
	d.metrics.record(ctx, op, time.Since(start), err)
	// A request cancelled because its caller went away (a client
	// disconnecting mid-Reserve) says nothing about the table, and must not
	// reset the breaker's count.
	if op == opReserve && !errors.Is(err, context.Canceled) {
		d.breaker.record(err)
	}
	return err
}

// dynamoStore is one tenant's view of the table.
type dynamoStore struct {
	d      *Dynamo
	prefix []byte
}

// Reserve puts every key's pending item in parallel, each conditional on no
// live item holding the key. A failed condition hands back the live item,
// whose state says Duplicate or InFlight without a read, or Claimed when its
// token is the put's own. On any error it releases, by token, every put that
// may have landed; what that undo misses (below) holds its key InFlight
// until the lease ends, as a crashed request's claim does.
func (s *dynamoStore) Reserve(ctx context.Context, keys []Key, lease time.Duration) ([]Claim, error) {
	if len(keys) == 0 {
		return []Claim{}, nil
	}
	if err := s.d.breaker.allow(); err != nil {
		s.d.metrics.shortCircuit(ctx)
		return nil, err
	}
	now := s.d.now()
	nowSec := strconv.FormatInt(now.Unix(), 10)
	exp := expiresAt(now, lease)
	claims := make([]Claim, len(keys))
	tried := make([]Claim, len(keys))
	sent := make([]bool, len(keys))
	// The first failure skips the puts not yet sent: the Reserve fails
	// either way, and a throttled table should not take the rest. A put
	// already sent runs on ctx, not gctx, so a sibling's failure never cuts
	// it off: it answers before the undo below. The caller's cancellation or
	// the put's own deadline can, and DynamoDB may then apply it after its
	// release.
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.d.cfg.ReserveConcurrency)
	for i, k := range keys {
		token := newToken()
		tried[i] = Claim{Key: k, Status: Claimed, Token: token}
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}
			sent[i] = true
			status, err := s.reserve(ctx, k, token, nowSec, exp)
			if err != nil {
				return err
			}
			claims[i] = Claim{Key: k, Status: status}
			if status == Claimed {
				claims[i].Token = token
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		// A sent put that errored may have landed anyway; releasing a key
		// its token does not hold is a no-op. Best effort: a put applied
		// after this, or a release that fails, lapses with the lease.
		var undo []Claim
		for i, c := range claims {
			if sent[i] && (c.Status == Claimed || c.Status == 0) {
				undo = append(undo, tried[i])
			}
		}
		_ = s.Release(context.WithoutCancel(ctx), undo)
		return nil, err
	}
	return claims, nil
}

func (s *dynamoStore) reserve(ctx context.Context, k Key, token, nowSec string, exp int64) (Status, error) {
	item := map[string]types.AttributeValue{
		attrKey:    &types.AttributeValueMemberS{Value: string(AppendKey(nil, s.prefix, k))},
		attrState:  &types.AttributeValueMemberN{Value: statePending},
		attrExpiry: &types.AttributeValueMemberN{Value: strconv.FormatInt(exp, 10)},
		attrToken:  &types.AttributeValueMemberB{Value: []byte(token)},
	}
	err := s.d.call(ctx, opReserve, func(ctx context.Context) error {
		_, err := s.d.api.PutItem(ctx, &dynamodb.PutItemInput{
			TableName:                           &s.d.cfg.Table,
			Item:                                item,
			ConditionExpression:                 aws.String(condReserve),
			ExpressionAttributeValues:           map[string]types.AttributeValue{":now": &types.AttributeValueMemberN{Value: nowSec}},
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		})
		return err
	})
	var held *types.ConditionalCheckFailedException
	if !errors.As(err, &held) {
		if err != nil {
			return 0, err
		}
		return Claimed, nil
	}
	st, _ := held.Item[attrState].(*types.AttributeValueMemberN)
	switch {
	case st != nil && st.Value == stateCommitted:
		return Duplicate, nil
	case st != nil && st.Value == statePending && heldBy(held.Item, token):
		// This put's own item: an SDK retry of an attempt that was applied
		// but whose answer was lost (a 500, a connection reset).
		return Claimed, nil
	}
	return InFlight, nil
}

func heldBy(item map[string]types.AttributeValue, token string) bool {
	tk, ok := item[attrToken].(*types.AttributeValueMemberB)
	return ok && string(tk.Value) == token
}

// Commit overwrites every claim's item as committed, unconditionally, 25 to a
// BatchWriteItem, retrying the items DynamoDB leaves unprocessed and a batch
// that failed transiently.
func (s *dynamoStore) Commit(ctx context.Context, claims []Claim, retention time.Duration) error {
	if len(claims) == 0 {
		return nil
	}
	var ex types.AttributeValue
	if retention > 0 {
		ex = &types.AttributeValueMemberN{Value: strconv.FormatInt(expiresAt(s.d.now(), retention), 10)}
	}
	// BatchWriteItem refuses a key twice in one call; a caller merging
	// claims from two Reserves could hand one over twice.
	seen := make(map[string]bool, len(claims))
	writes := make([]types.WriteRequest, 0, len(claims))
	for _, c := range claims {
		pk := string(AppendKey(nil, s.prefix, c.Key))
		if seen[pk] {
			continue
		}
		seen[pk] = true
		item := map[string]types.AttributeValue{
			attrKey:   &types.AttributeValueMemberS{Value: pk},
			attrState: &types.AttributeValueMemberN{Value: stateCommitted},
			attrToken: &types.AttributeValueMemberB{Value: []byte(c.Token)},
		}
		if ex != nil {
			item[attrExpiry] = ex
		}
		writes = append(writes, types.WriteRequest{PutRequest: &types.PutRequest{Item: item}})
	}
	// Every chunk is attempted whatever another's fate: these records are
	// already published, and an uncommitted id lets a retry publish again.
	chunks := (len(writes) + batchWriteMax - 1) / batchWriteMax
	return forEach(chunks, s.d.cfg.ReserveConcurrency, func(i int) error {
		return s.commitChunk(ctx, writes[i*batchWriteMax:min((i+1)*batchWriteMax, len(writes))])
	})
}

func (s *dynamoStore) commitChunk(ctx context.Context, writes []types.WriteRequest) error {
	for attempt := 0; ; attempt++ {
		var unprocessed []types.WriteRequest
		err := s.d.call(ctx, "batch_write_item", func(ctx context.Context) error {
			out, err := s.d.api.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{s.d.cfg.Table: writes},
			})
			if err == nil {
				unprocessed = out.UnprocessedItems[s.d.cfg.Table]
			}
			return err
		})
		switch {
		case err == nil:
		case errors.Is(err, ErrUnavailable):
			// DynamoDB throttles a batch whole only when it processed none of
			// it, and a timeout leaves its fate unknown: retry it whole, as a
			// round that left every item unprocessed. The puts are idempotent.
			unprocessed = writes
		default:
			return err
		}
		if len(unprocessed) == 0 {
			return nil
		}
		if attempt+1 >= commitRounds {
			if err != nil {
				return err
			}
			return fmt.Errorf("%w: dynamodb batch_write_item: %d items still unprocessed", ErrUnavailable, len(unprocessed))
		}
		if err == nil {
			s.d.metrics.unprocessed.Add(ctx, int64(len(unprocessed)))
		}
		writes = unprocessed
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.d.commitBackoff(attempt)):
		}
	}
}

// Release deletes each claim's item only while it is still that claim's
// pending item; a failed condition means the key lapsed, was re-claimed or
// was committed, and is left alone.
func (s *dynamoStore) Release(ctx context.Context, claims []Claim) error {
	if len(claims) == 0 {
		return nil
	}
	// Every claim is attempted: one left behind holds its id for a lease.
	return forEach(len(claims), s.d.cfg.ReserveConcurrency, func(i int) error {
		c := claims[i]
		err := s.d.call(ctx, "delete_item", func(ctx context.Context) error {
			_, err := s.d.api.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName:           &s.d.cfg.Table,
				Key:                 map[string]types.AttributeValue{attrKey: &types.AttributeValueMemberS{Value: string(AppendKey(nil, s.prefix, c.Key))}},
				ConditionExpression: aws.String(condRelease),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":tk":      &types.AttributeValueMemberB{Value: []byte(c.Token)},
					":pending": &types.AttributeValueMemberN{Value: statePending},
				},
			})
			return err
		})
		var gone *types.ConditionalCheckFailedException
		if errors.As(err, &gone) {
			return nil
		}
		return err
	})
}

// Close is a no-op: the client is the Dynamo's, shared by every tenant.
func (s *dynamoStore) Close() error { return nil }

// forEach runs do for every index, at most limit at once, and joins the
// errors: one failure never stops the rest.
func forEach(n, limit int, do func(i int) error) error {
	errs := make([]error, n)
	var g errgroup.Group
	g.SetLimit(limit)
	for i := range n {
		g.Go(func() error { errs[i] = do(i); return nil })
	}
	_ = g.Wait()
	return errors.Join(errs...)
}

// expiresAt is t+d in epoch seconds rounded up, so a claim or commit never
// ends before it was asked to: TTL attributes are whole seconds.
func expiresAt(t time.Time, d time.Duration) int64 {
	end := t.Add(d)
	sec := end.Unix()
	if end.Nanosecond() > 0 {
		sec++
	}
	return sec
}

func newToken() string {
	b := make([]byte, tokenBytes)
	_, _ = rand.Read(b) // crypto/rand.Read never fails
	return string(b)
}

// classify maps a DynamoDB error onto the contract: a condition failure is
// returned as is for the caller to read, anything retrying later can cure
// wraps ErrUnavailable (503), and the rest — a missing table, denied access,
// a malformed request — is a configuration bug (500).
func classify(op string, err error) error {
	if err == nil {
		return nil
	}
	var cond *types.ConditionalCheckFailedException
	if errors.As(err, &cond) {
		return err
	}
	if transient(err) {
		return fmt.Errorf("%w: dynamodb %s: %w", ErrUnavailable, op, err)
	}
	return fmt.Errorf("dynamodb %s: %w", op, err)
}

func transient(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if (retry.RetryableConnectionError{}).IsErrorRetryable(err) == aws.TrueTernary {
		return true
	}
	var api smithy.APIError
	if !errors.As(err, &api) {
		return false
	}
	code := api.ErrorCode()
	if _, ok := retry.DefaultThrottleErrorCodes[code]; ok {
		return true
	}
	if _, ok := retry.DefaultRetryableErrorCodes[code]; ok {
		return true
	}
	switch code {
	case "InternalServerError", "ServiceUnavailable", "ReplicatedWriteConflictException":
		return true
	}
	return api.ErrorFault() == smithy.FaultServer
}

// breaker short-circuits Reserve for a second after breakerTrips consecutive
// unavailable answers inside a second, so a throttled or unreachable table
// fails requests fast instead of spending every one's full timeout.
type breaker struct {
	mu        sync.Mutex
	now       func() time.Time
	fails     int
	since     time.Time
	openUntil time.Time
}

const (
	breakerTrips  = 5
	breakerWindow = time.Second
	breakerCool   = time.Second
)

var errBreakerOpen = fmt.Errorf("%w: dynamodb is failing; short-circuited", ErrUnavailable)

func newBreaker(now func() time.Time) *breaker { return &breaker{now: now} }

func (b *breaker) allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.now().Before(b.openUntil) {
		return errBreakerOpen
	}
	return nil
}

func (b *breaker) record(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !errors.Is(err, ErrUnavailable) {
		b.fails = 0
		return
	}
	now := b.now()
	if b.fails == 0 || now.Sub(b.since) > breakerWindow {
		b.fails, b.since = 0, now
	}
	b.fails++
	if b.fails >= breakerTrips {
		b.fails = 0
		b.openUntil = now.Add(breakerCool)
	}
}

type dynamoMetrics struct {
	requests    metric.Int64Counter
	duration    metric.Float64Histogram
	unprocessed metric.Int64Counter
	shorted     metric.Int64Counter
}

func newDynamoMetrics() dynamoMetrics {
	meter := otel.Meter("wavehouse-dedupe")
	requests, _ := meter.Int64Counter("wavehouse_dedupe_dynamodb_requests_total",
		metric.WithDescription("DynamoDB dedupe requests by operation and outcome (ok, condition_failed, unavailable, canceled, error)"))
	duration, _ := meter.Float64Histogram("wavehouse_dedupe_dynamodb_request_duration_seconds",
		metric.WithDescription("DynamoDB dedupe request latency, SDK retries included"), metric.WithUnit("s"))
	unprocessed, _ := meter.Int64Counter("wavehouse_dedupe_dynamodb_unprocessed_items_total",
		metric.WithDescription("Commit items DynamoDB left unprocessed and the backend retried"))
	shorted, _ := meter.Int64Counter("wavehouse_dedupe_dynamodb_short_circuits_total",
		metric.WithDescription("Reserves refused without a request while DynamoDB was failing"))
	return dynamoMetrics{requests: requests, duration: duration, unprocessed: unprocessed, shorted: shorted}
}

func (m dynamoMetrics) record(ctx context.Context, op string, took time.Duration, err error) {
	outcome := "ok"
	var cond *types.ConditionalCheckFailedException
	switch {
	case err == nil:
	case errors.As(err, &cond):
		outcome = "condition_failed"
	case errors.Is(err, context.Canceled):
		outcome = "canceled"
	case errors.Is(err, ErrUnavailable):
		outcome = "unavailable"
	default:
		outcome = "error"
	}
	ctx = context.WithoutCancel(ctx)
	m.requests.Add(ctx, 1, metric.WithAttributes(attribute.String("op", op), attribute.String("outcome", outcome)))
	m.duration.Record(ctx, took.Seconds(), metric.WithAttributes(attribute.String("op", op)))
}

func (m dynamoMetrics) shortCircuit(ctx context.Context) { m.shorted.Add(ctx, 1) }

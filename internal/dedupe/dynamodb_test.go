package dedupe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDynamo answers each operation through its func, or with success when
// that is nil. The DynamoDB semantics themselves are tested against
// dynamodb-local (tests/integration); this is for the error paths it cannot
// produce.
type fakeDynamo struct {
	put      func(context.Context, *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	batch    func(context.Context, *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error)
	del      func(context.Context, *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)
	describe func() (*dynamodb.DescribeTableOutput, error)
	ttl      func() (*dynamodb.DescribeTimeToLiveOutput, error)
}

func (f *fakeDynamo) PutItem(ctx context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if f.put == nil {
		return &dynamodb.PutItemOutput{}, nil
	}
	return f.put(ctx, in)
}

func (f *fakeDynamo) BatchWriteItem(ctx context.Context, in *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.batch == nil {
		return &dynamodb.BatchWriteItemOutput{}, nil
	}
	return f.batch(ctx, in)
}

func (f *fakeDynamo) DeleteItem(ctx context.Context, in *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.del == nil {
		return &dynamodb.DeleteItemOutput{}, nil
	}
	return f.del(ctx, in)
}

func (f *fakeDynamo) DescribeTable(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	return f.describe()
}

func (f *fakeDynamo) DescribeTimeToLive(context.Context, *dynamodb.DescribeTimeToLiveInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
	return f.ttl()
}

func (f *fakeDynamo) CreateTable(context.Context, *dynamodb.CreateTableInput, ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	return nil, errors.New("not used")
}

func (f *fakeDynamo) UpdateTimeToLive(context.Context, *dynamodb.UpdateTimeToLiveInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateTimeToLiveOutput, error) {
	return nil, errors.New("not used")
}

func apiErr(code string, fault smithy.ErrorFault) error {
	return &smithy.GenericAPIError{Code: code, Message: "injected", Fault: fault}
}

func openFake(t *testing.T, f *fakeDynamo) (*Dynamo, Deduplicator) {
	t.Helper()
	return openFakeWith(t, f, DynamoConfig{Table: "dedupe"})
}

func openFakeWith(t *testing.T, f *fakeDynamo, cfg DynamoConfig) (*Dynamo, Deduplicator) {
	t.Helper()
	d := newDynamo(f, cfg)
	d.commitBackoff = func(int) time.Duration { return 0 }
	m := d.Tenant("acme")
	require.NoError(t, m.Apply(true))
	t.Cleanup(func() { _ = m.Close() })
	return d, m
}

func keys(ids ...string) []Key {
	out := make([]Key, len(ids))
	for i, id := range ids {
		out[i] = Key{Table: "events", ID: id}
	}
	return out
}

func TestClassify(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		err         error
		unavailable bool
	}{
		{"throttled", apiErr("ThrottlingException", smithy.FaultClient), true},
		{"over provisioned throughput", &types.ProvisionedThroughputExceededException{}, true},
		{"account request limit", apiErr("RequestLimitExceeded", smithy.FaultClient), true},
		{"internal error", &types.InternalServerError{}, true},
		{"unknown server fault", apiErr("Whatever", smithy.FaultServer), true},
		{"request timeout", apiErr("RequestTimeoutException", smithy.FaultClient), true},
		{"multi-region write conflict", &types.ReplicatedWriteConflictException{}, true},
		{"deadline", fmt.Errorf("op: %w", context.DeadlineExceeded), true},
		{"connection refused", &smithyhttp.RequestSendError{Err: &net.OpError{Op: "dial", Err: errors.New("refused")}}, true},
		{"missing table", &types.ResourceNotFoundException{}, false},
		{"access denied", apiErr("AccessDeniedException", smithy.FaultClient), false},
		{"validation", apiErr("ValidationException", smithy.FaultClient), false},
		{"caller went away", context.Canceled, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := classify("put_item", tc.err)
			assert.ErrorIs(t, err, tc.err, "the cause stays reachable")
			assert.Equal(t, tc.unavailable, errors.Is(err, ErrUnavailable))
		})
	}
	assert.NoError(t, classify("put_item", nil))
	ccf := &types.ConditionalCheckFailedException{}
	assert.Same(t, error(ccf), classify("put_item", ccf), "a condition failure is an answer, not an error")
}

func TestDynamo_ReserveReadsTheHeldItem(t *testing.T) {
	t.Parallel()
	_, m := openFake(t, &fakeDynamo{put: func(_ context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
		id := in.Item[attrKey].(*types.AttributeValueMemberS).Value
		switch id[len(id)-1] {
		case 'd':
			return nil, &types.ConditionalCheckFailedException{Item: map[string]types.AttributeValue{attrState: &types.AttributeValueMemberN{Value: stateCommitted}}}
		case 'f':
			return nil, &types.ConditionalCheckFailedException{Item: map[string]types.AttributeValue{attrState: &types.AttributeValueMemberN{Value: statePending}}}
		}
		assert.Equal(t, condReserve, aws.ToString(in.ConditionExpression))
		assert.Equal(t, types.ReturnValuesOnConditionCheckFailureAllOld, in.ReturnValuesOnConditionCheckFailure)
		return &dynamodb.PutItemOutput{}, nil
	}})
	claims, err := m.Reserve(t.Context(), keys("new", "old", "inf"), time.Minute)
	require.NoError(t, err)
	assert.Equal(t, []Status{Claimed, Duplicate, InFlight}, []Status{claims[0].Status, claims[1].Status, claims[2].Status})
	assert.Len(t, claims[0].Token, tokenBytes)
	assert.Empty(t, claims[1].Token)
}

// An SDK retry of a put whose first attempt was applied fails its condition
// on the put's own item: that is the caller's claim, not another request's.
func TestDynamo_RetriedPutKeepsItsOwnClaim(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	putTokens := map[string]string{}
	var released []string
	fake := &fakeDynamo{
		put: func(_ context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			id := idOf(in.Item[attrKey])
			mu.Lock()
			putTokens[id] = string(in.Item[attrToken].(*types.AttributeValueMemberB).Value)
			mu.Unlock()
			switch id {
			case "k0":
				return nil, &types.ConditionalCheckFailedException{Item: in.Item}
			case "k1":
				theirs := maps.Clone(in.Item)
				theirs[attrToken] = &types.AttributeValueMemberB{Value: []byte("theirs")}
				return nil, &types.ConditionalCheckFailedException{Item: theirs}
			}
			return nil, &types.InternalServerError{}
		},
		del: func(_ context.Context, in *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
			id := idOf(in.Key[attrKey])
			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, putTokens[id], string(in.ExpressionAttributeValues[":tk"].(*types.AttributeValueMemberB).Value))
			released = append(released, id)
			return &dynamodb.DeleteItemOutput{}, nil
		},
	}
	_, m := openFakeWith(t, fake, DynamoConfig{Table: "dedupe", ReserveConcurrency: 1})

	claims, err := m.Reserve(t.Context(), keys("k0", "k1"), time.Minute)
	require.NoError(t, err)
	assert.Equal(t, []Status{Claimed, InFlight}, []Status{claims[0].Status, claims[1].Status}, "a pending item is InFlight only under another token")
	assert.Equal(t, putTokens["k0"], claims[0].Token)

	// k0 is sent and answered before k2 fails, so the undo owns it.
	_, err = m.Reserve(t.Context(), keys("k0", "k2"), time.Minute)
	require.ErrorIs(t, err, ErrUnavailable)
	mu.Lock()
	defer mu.Unlock()
	assert.Contains(t, released, "k0", "the failed Reserve's undo releases the retried put's own item")
}

func TestDynamo_FailedReserveReleasesEveryPutThatMayHaveLanded(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	putTokens := map[string]string{}
	var released []string
	_, m := openFake(t, &fakeDynamo{
		put: func(_ context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			id := in.Item[attrKey].(*types.AttributeValueMemberS).Value
			mu.Lock()
			putTokens[id] = string(in.Item[attrToken].(*types.AttributeValueMemberB).Value)
			mu.Unlock()
			switch id[len(id)-3:] {
			case "dup":
				return nil, &types.ConditionalCheckFailedException{Item: map[string]types.AttributeValue{attrState: &types.AttributeValueMemberN{Value: stateCommitted}}}
			case "bad":
				return nil, &types.InternalServerError{}
			}
			return &dynamodb.PutItemOutput{}, nil
		},
		del: func(_ context.Context, in *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
			id := in.Key[attrKey].(*types.AttributeValueMemberS).Value
			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, putTokens[id], string(in.ExpressionAttributeValues[":tk"].(*types.AttributeValueMemberB).Value), "released by the token it was put with")
			released = append(released, id[len(id)-3:])
			return &dynamodb.DeleteItemOutput{}, nil
		},
	})
	_, err := m.Reserve(t.Context(), keys("ok1", "dup", "bad", "ok2"), time.Minute)
	require.ErrorIs(t, err, ErrUnavailable)
	var sent []string
	for id := range putTokens {
		if id[len(id)-3:] != "dup" {
			sent = append(sent, id[len(id)-3:])
		}
	}
	assert.ElementsMatch(t, sent, released, "every sent put but the duplicate, which was never ours")
	assert.Contains(t, released, "bad", "the failed put may have landed")
}

func TestDynamo_CommitRetriesUnprocessedItems(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	var mu sync.Mutex
	written := map[string]int{}
	heldBack := map[string]bool{}
	_, m := openFake(t, &fakeDynamo{batch: func(_ context.Context, in *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
		calls.Add(1)
		reqs := in.RequestItems["dedupe"]
		assert.LessOrEqual(t, len(reqs), batchWriteMax)
		// Leave the last item of every call unprocessed once.
		mu.Lock()
		defer mu.Unlock()
		var left []types.WriteRequest
		for i, r := range reqs {
			pk := r.PutRequest.Item[attrKey].(*types.AttributeValueMemberS).Value
			assert.Equal(t, stateCommitted, r.PutRequest.Item[attrState].(*types.AttributeValueMemberN).Value)
			assert.Contains(t, r.PutRequest.Item, attrExpiry)
			if i == len(reqs)-1 && !heldBack[pk] && len(reqs) > 1 {
				heldBack[pk] = true
				left = append(left, r)
				continue
			}
			written[pk]++
		}
		return &dynamodb.BatchWriteItemOutput{UnprocessedItems: map[string][]types.WriteRequest{"dedupe": left}}, nil
	}})
	ids := make([]string, 60)
	for i := range ids {
		ids[i] = fmt.Sprint(i)
	}
	claims := make([]Claim, 0, len(ids)+1)
	for _, k := range keys(ids...) {
		claims = append(claims, Claim{Key: k, Status: Claimed, Token: "t"})
	}
	claims = append(claims, claims[0])
	require.NoError(t, m.Commit(t.Context(), claims, time.Hour))
	assert.Len(t, written, 60, "a key handed over twice is written once")
	for pk, n := range written {
		assert.Equal(t, 1, n, "%q", pk)
	}
	assert.Equal(t, int64(6), calls.Load(), "3 chunks, each retried once")
}

func TestDynamo_CommitGivesUpOnItemsThatStayUnprocessed(t *testing.T) {
	t.Parallel()
	_, m := openFake(t, &fakeDynamo{batch: func(_ context.Context, in *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
		return &dynamodb.BatchWriteItemOutput{UnprocessedItems: in.RequestItems}, nil
	}})
	err := m.Commit(t.Context(), []Claim{{Key: keys("a")[0], Status: Claimed, Token: "t"}}, 0)
	require.ErrorIs(t, err, ErrUnavailable)
}

// DynamoDB answers a BatchWriteItem it processed none of with a throttle,
// not with every item unprocessed: the chunk gets the same rounds either way.
func TestDynamo_CommitRetriesAFailedBatch(t *testing.T) {
	t.Parallel()
	throttle := &types.ProvisionedThroughputExceededException{}
	for _, tc := range []struct {
		name        string
		fails       int
		err         error
		calls       int64
		committed   bool
		unavailable bool
	}{
		{"throttled, then through", 3, throttle, 4, true, false},
		{"throttled every round", 1 << 10, throttle, commitRounds, false, true},
		{"timed out, then through", 1, fmt.Errorf("op: %w", context.DeadlineExceeded), 2, true, false},
		{"a configuration bug is final", 1 << 10, &types.ResourceNotFoundException{}, 1, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			_, m := openFake(t, &fakeDynamo{batch: func(_ context.Context, in *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
				assert.Len(t, in.RequestItems["dedupe"], 2, "the whole batch, every round")
				if calls.Add(1) <= int64(tc.fails) {
					return nil, tc.err
				}
				return &dynamodb.BatchWriteItemOutput{}, nil
			}})
			var claims []Claim
			for _, k := range keys("a", "b") {
				claims = append(claims, Claim{Key: k, Status: Claimed, Token: "t"})
			}
			err := m.Commit(t.Context(), claims, 0)
			assert.Equal(t, tc.calls, calls.Load())
			if tc.committed {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.err, "the last round's cause is kept")
			assert.Equal(t, tc.unavailable, errors.Is(err, ErrUnavailable))
		})
	}
}

func TestDynamo_ReleaseTreatsAFailedConditionAsDone(t *testing.T) {
	t.Parallel()
	_, m := openFake(t, &fakeDynamo{del: func(_ context.Context, in *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
		assert.Equal(t, condRelease, aws.ToString(in.ConditionExpression))
		id := in.Key[attrKey].(*types.AttributeValueMemberS).Value
		if id[len(id)-1] == 'x' {
			return nil, &types.ResourceNotFoundException{}
		}
		return nil, &types.ConditionalCheckFailedException{}
	}})
	claim := func(id string) []Claim { return []Claim{{Key: keys(id)[0], Status: Claimed, Token: "t"}} }
	require.NoError(t, m.Release(t.Context(), claim("gone")))
	err := m.Release(t.Context(), claim("x"))
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrUnavailable))
}

func TestDynamo_BreakerShortCircuitsReserve(t *testing.T) {
	t.Parallel()
	var puts atomic.Int64
	var down atomic.Bool
	down.Store(true)
	d, m := openFake(t, &fakeDynamo{put: func(context.Context, *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
		puts.Add(1)
		if down.Load() {
			return nil, &types.ProvisionedThroughputExceededException{}
		}
		return &dynamodb.PutItemOutput{}, nil
	}})
	now := time.Unix(1_000_000, 0)
	var clock sync.Mutex
	d.breaker.now = func() time.Time { clock.Lock(); defer clock.Unlock(); return now }
	for range breakerTrips {
		_, err := m.Reserve(t.Context(), keys("a"), time.Minute)
		require.ErrorIs(t, err, ErrUnavailable)
	}
	_, err := m.Reserve(t.Context(), keys("a"), time.Minute)
	require.ErrorIs(t, err, errBreakerOpen)
	assert.Equal(t, int64(breakerTrips), puts.Load(), "the open breaker sent nothing")

	down.Store(false)
	clock.Lock()
	now = now.Add(breakerCool)
	clock.Unlock()
	c, err := m.Reserve(t.Context(), keys("a"), time.Minute)
	require.NoError(t, err, "it closes after the cool-down")
	assert.Equal(t, Claimed, c[0].Status)
}

func TestBreaker_FailuresSpreadOutDoNotTrip(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_000_000, 0)
	b := newBreaker(func() time.Time { return now })
	fail := fmt.Errorf("%w: x", ErrUnavailable)
	for range 3 * breakerTrips {
		b.record(fail)
		now = now.Add(breakerWindow/(breakerTrips-1) + time.Millisecond)
	}
	require.NoError(t, b.allow())
	now = now.Add(2 * breakerWindow)
	for range breakerTrips - 1 {
		b.record(fail)
	}
	b.record(nil)
	b.record(fail)
	require.NoError(t, b.allow(), "a success resets the count")
}

func TestDynamo_Check(t *testing.T) {
	t.Parallel()
	good := &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
	}}
	ttlOn := &dynamodb.DescribeTimeToLiveOutput{TimeToLiveDescription: &types.TimeToLiveDescription{
		AttributeName: aws.String("ex"), TimeToLiveStatus: types.TimeToLiveStatusEnabled,
	}}
	check := func(table *dynamodb.DescribeTableOutput, ttl *dynamodb.DescribeTimeToLiveOutput, ttlErr error) error {
		f := &fakeDynamo{
			describe: func() (*dynamodb.DescribeTableOutput, error) { return table, nil },
			ttl:      func() (*dynamodb.DescribeTimeToLiveOutput, error) { return ttl, ttlErr },
		}
		return newDynamo(f, DynamoConfig{Table: "dedupe"}).Check(t.Context())
	}
	require.NoError(t, check(good, ttlOn, nil))
	require.NoError(t, check(good, &dynamodb.DescribeTimeToLiveOutput{}, nil), "no TTL is a warning")
	require.ErrorIs(t, check(good, nil, &types.InternalServerError{}), ErrUnavailable)

	withRange := &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
	}}
	assert.ErrorContains(t, check(withRange, ttlOn, nil), "key schema")
}

func TestDynamo_Config(t *testing.T) {
	t.Parallel()
	c := DynamoConfig{}.withDefaults()
	assert.Equal(t, DynamoConfig{Timeout: 250 * time.Millisecond, MaxAttempts: 3, RetryMode: "standard", ReserveConcurrency: 64}, c)
	for _, mode := range []string{"standard", "adaptive"} {
		r, err := newRetryer(DynamoConfig{RetryMode: mode, MaxAttempts: 4})
		require.NoError(t, err)
		assert.Equal(t, 4, r().MaxAttempts())
	}
	_, err := newRetryer(DynamoConfig{RetryMode: "legacy"})
	require.Error(t, err)

	_, err = NewDynamo(t.Context(), DynamoConfig{})
	require.ErrorContains(t, err, "table is required")
	_, err = NewDynamo(t.Context(), DynamoConfig{Table: "t", RetryMode: "legacy"})
	require.Error(t, err)
	d, err := NewDynamo(t.Context(), DynamoConfig{Table: "t", Region: "us-east-1"})
	require.NoError(t, err)
	require.ErrorIs(t, d.CreateTable(t.Context()), ErrCreateTableNeedsEndpoint, "never against real AWS")
}

func TestFullJitter(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		attempt int
		most    time.Duration
	}{
		{-1, 10 * time.Millisecond},
		{0, 10 * time.Millisecond},
		{2, 40 * time.Millisecond},
		{3, 50 * time.Millisecond},
		{1 << 20, 50 * time.Millisecond},
	} {
		seen := map[time.Duration]bool{}
		for range 200 {
			d := fullJitter(10*time.Millisecond, 50*time.Millisecond, tc.attempt)
			assert.GreaterOrEqual(t, d, time.Duration(0))
			assert.LessOrEqual(t, d, tc.most, "attempt %d", tc.attempt)
			seen[d] = true
		}
		assert.Greater(t, len(seen), 1, "attempt %d: jittered, not a fixed wait", tc.attempt)
	}
	assert.Zero(t, fullJitter(10*time.Millisecond, 0, 3))
}

// Whatever the Timeout and MaxAttempts, the SDK's retries of one call wait
// at most half the Timeout between them.
func TestRetryBackoff_FitsTheTimeout(t *testing.T) {
	t.Parallel()
	for _, cfg := range []DynamoConfig{
		{},
		{MaxAttempts: 10},
		{Timeout: 5 * time.Second, MaxAttempts: 2},
		{Timeout: 40 * time.Millisecond, MaxAttempts: 5},
	} {
		cfg = cfg.withDefaults()
		r, err := newRetryer(cfg)
		require.NoError(t, err)
		retryer := r()
		var worst time.Duration
		// The SDK numbers a call's retries from 1 (from 0 under its 2026
		// retry behaviour); the later ones are the longer.
		for attempt := 1; attempt < cfg.MaxAttempts; attempt++ {
			var most time.Duration
			seen := map[time.Duration]bool{}
			for range 500 {
				d, err := retryer.RetryDelay(attempt, nil)
				require.NoError(t, err)
				most = max(most, d)
				seen[d] = true
			}
			assert.Greater(t, len(seen), 1, "retries are jittered, not in lockstep")
			worst += most
		}
		assert.LessOrEqual(t, worst, cfg.Timeout/2, "%+v", cfg)
	}
}

func TestDynamo_CommitBackoffIsJittered(t *testing.T) {
	t.Parallel()
	d := newDynamo(&fakeDynamo{}, DynamoConfig{Table: "dedupe"})
	for attempt := range commitRounds {
		seen := map[time.Duration]bool{}
		for range 200 {
			w := d.commitBackoff(attempt)
			assert.LessOrEqual(t, w, commitCeiling)
			seen[w] = true
		}
		assert.Greater(t, len(seen), 1, "round %d", attempt)
	}
}

// throttledHTTP answers every request with a DynamoDB throttle, counting
// the PutItems.
type throttledHTTP struct{ puts atomic.Int64 }

func (h *throttledHTTP) Do(r *http.Request) (*http.Response, error) {
	if r.Header.Get("X-Amz-Target") == "DynamoDB_20120810.PutItem" {
		h.puts.Add(1)
	}
	body := `{"__type":"com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException","message":"injected"}`
	return &http.Response{
		StatusCode: http.StatusBadRequest,
		Header:     http.Header{"Content-Type": {"application/x-amz-json-1.0"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}, nil
}

// Through the real SDK stack at the default Timeout and MaxAttempts: a
// throttled put is retried until its attempts run out, inside the call's
// deadline, so the Reserve fails with the throttle as its cause.
func TestDynamo_ThrottledCallEndsOnItsLastAttempt(t *testing.T) {
	t.Parallel()
	h := &throttledHTTP{}
	d, err := NewDynamo(t.Context(), DynamoConfig{Table: "dedupe", Region: "us-east-1", Endpoint: "http://dynamodb.invalid"},
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("k", "s", "")),
		config.WithHTTPClient(h))
	require.NoError(t, err)
	m := d.Tenant("acme")
	require.NoError(t, m.Apply(true))
	calls := breakerTrips - 1
	for range calls {
		start := time.Now()
		_, err := m.Reserve(t.Context(), keys("a"), time.Minute)
		took := time.Since(start)
		require.ErrorIs(t, err, ErrUnavailable)
		var maxed *retry.MaxAttemptsError
		require.ErrorAs(t, err, &maxed, "the attempts ran out, not the deadline")
		var throttled *types.ProvisionedThroughputExceededException
		require.ErrorAs(t, err, &throttled)
		require.NotErrorIs(t, err, context.DeadlineExceeded)
		assert.Less(t, took, d.cfg.Timeout)
	}
	assert.Equal(t, int64(calls*d.cfg.MaxAttempts), h.puts.Load())
}

// The idle pool holds a connection for every call one Reserve can have in
// flight, so a wide Reserve reuses them rather than dial; an HTTP client in
// extra (TestDynamo_ThrottledCallEndsOnItsLastAttempt's) replaces it.
func TestNewDynamo_SizesTheIdlePool(t *testing.T) {
	t.Parallel()
	for _, n := range []int{0, 8, 200} {
		d, err := NewDynamo(t.Context(), DynamoConfig{Table: "dedupe", Region: "us-east-1", ReserveConcurrency: n})
		require.NoError(t, err)
		client, ok := d.api.(*dynamodb.Client).Options().HTTPClient.(*awshttp.BuildableClient)
		require.True(t, ok)
		tr := client.GetTransport()
		assert.GreaterOrEqual(t, tr.MaxIdleConnsPerHost, d.cfg.ReserveConcurrency, "ReserveConcurrency %d", n)
		assert.GreaterOrEqual(t, tr.MaxIdleConns, d.cfg.ReserveConcurrency, "ReserveConcurrency %d", n)
	}
}

func TestExpiresAt(t *testing.T) {
	t.Parallel()
	base := time.Unix(100, 0)
	assert.Equal(t, int64(101), expiresAt(base, time.Second))
	assert.Equal(t, int64(102), expiresAt(base, 1500*time.Millisecond), "rounded up: never ends early")
	assert.Equal(t, int64(102), expiresAt(base.Add(time.Nanosecond), time.Second))
}

func idOf(av types.AttributeValue) string {
	s := av.(*types.AttributeValueMemberS).Value
	return s[len(s)-2:]
}

func TestDynamo_CommitAttemptsEveryChunk(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	written := 0
	_, m := openFakeWith(t, &fakeDynamo{batch: func(_ context.Context, in *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
		reqs := in.RequestItems["dedupe"]
		if idOf(reqs[0].PutRequest.Item[attrKey]) == "00" {
			return nil, &types.InternalServerError{}
		}
		mu.Lock()
		written += len(reqs)
		mu.Unlock()
		return &dynamodb.BatchWriteItemOutput{}, nil
	}}, DynamoConfig{Table: "dedupe", ReserveConcurrency: 1})
	var claims []Claim
	for i := range 3 * batchWriteMax {
		claims = append(claims, Claim{Key: keys(fmt.Sprintf("%02d", i))[0], Status: Claimed, Token: "t"})
	}
	require.ErrorIs(t, m.Commit(t.Context(), claims, 0), ErrUnavailable)
	assert.Equal(t, 2*batchWriteMax, written, "a failed chunk does not cancel the others: their records are published")
}

// A put cancelled by its caller is not an answer from the table: it does
// not reset the breaker's count of throttled puts.
func TestDynamo_CallerCancelDoesNotResetBreaker(t *testing.T) {
	t.Parallel()
	var hang atomic.Bool
	started := make(chan struct{}, 1)
	_, m := openFake(t, &fakeDynamo{put: func(ctx context.Context, _ *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
		if hang.Load() {
			started <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return nil, &types.ProvisionedThroughputExceededException{}
	}})
	for range breakerTrips - 1 {
		_, err := m.Reserve(t.Context(), keys("a"), time.Minute)
		require.ErrorIs(t, err, ErrUnavailable)
	}
	hang.Store(true)
	ctx, cancel := context.WithCancel(t.Context())
	go func() { <-started; cancel() }()
	_, err := m.Reserve(ctx, keys("a"), time.Minute)
	require.ErrorIs(t, err, context.Canceled)
	hang.Store(false)
	_, err = m.Reserve(t.Context(), keys("a"), time.Minute)
	require.ErrorIs(t, err, ErrUnavailable)
	_, err = m.Reserve(t.Context(), keys("a"), time.Minute)
	require.ErrorIs(t, err, errBreakerOpen, "the cancelled put did not reset the count")
}

func TestDynamo_ReleaseAttemptsEveryClaim(t *testing.T) {
	t.Parallel()
	var deletes atomic.Int64
	_, m := openFakeWith(t, &fakeDynamo{del: func(_ context.Context, in *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
		deletes.Add(1)
		if idOf(in.Key[attrKey]) == "k0" {
			return nil, &types.ProvisionedThroughputExceededException{}
		}
		return &dynamodb.DeleteItemOutput{}, nil
	}}, DynamoConfig{Table: "dedupe", ReserveConcurrency: 1})
	var claims []Claim
	for _, k := range keys("k0", "k1", "k2", "k3") {
		claims = append(claims, Claim{Key: k, Status: Claimed, Token: "t"})
	}
	require.ErrorIs(t, m.Release(t.Context(), claims), ErrUnavailable)
	assert.Equal(t, int64(4), deletes.Load())
}

// One throttled put in a multi-key Reserve: the unsent puts are never sent,
// and a sibling already sent runs to its answer before the undo releases it,
// so a put cannot land after its own release.
func TestDynamo_FailedMultiKeyReserve(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var put, released []string
	landed := map[string]bool{}
	_, m := openFakeWith(t, &fakeDynamo{
		put: func(ctx context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			id := idOf(in.Item[attrKey])
			mu.Lock()
			put = append(put, id)
			mu.Unlock()
			if id == "k0" {
				return nil, &types.ProvisionedThroughputExceededException{}
			}
			time.Sleep(20 * time.Millisecond)
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			mu.Lock()
			landed[id] = true
			mu.Unlock()
			return &dynamodb.PutItemOutput{}, nil
		},
		del: func(_ context.Context, in *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
			id := idOf(in.Key[attrKey])
			mu.Lock()
			defer mu.Unlock()
			released = append(released, id)
			if id != "k0" && !landed[id] {
				t.Errorf("%s released before its put answered", id)
			}
			return &dynamodb.DeleteItemOutput{}, nil
		},
	}, DynamoConfig{Table: "dedupe", ReserveConcurrency: 2})
	ks := keys("k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7")
	_, err := m.Reserve(t.Context(), ks, time.Minute)
	require.ErrorIs(t, err, ErrUnavailable)
	mu.Lock()
	defer mu.Unlock()
	assert.ElementsMatch(t, put, released, "exactly the sent puts are released")
	assert.Less(t, len(put), len(ks), "unsent puts were never sent")
}

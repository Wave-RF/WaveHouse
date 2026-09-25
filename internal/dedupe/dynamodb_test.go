package dedupe

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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
	put      func(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	batch    func(*dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error)
	del      func(*dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)
	describe func() (*dynamodb.DescribeTableOutput, error)
	ttl      func() (*dynamodb.DescribeTimeToLiveOutput, error)
}

func (f *fakeDynamo) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if f.put == nil {
		return &dynamodb.PutItemOutput{}, nil
	}
	return f.put(in)
}

func (f *fakeDynamo) BatchWriteItem(_ context.Context, in *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	if f.batch == nil {
		return &dynamodb.BatchWriteItemOutput{}, nil
	}
	return f.batch(in)
}

func (f *fakeDynamo) DeleteItem(_ context.Context, in *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if f.del == nil {
		return &dynamodb.DeleteItemOutput{}, nil
	}
	return f.del(in)
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
	d := newDynamo(f, DynamoConfig{Table: "dedupe"})
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
	_, m := openFake(t, &fakeDynamo{put: func(in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
		id := string(in.Item[attrKey].(*types.AttributeValueMemberB).Value)
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

func TestDynamo_FailedReserveReleasesEveryPutThatMayHaveLanded(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	putTokens := map[string]string{}
	var released []string
	_, m := openFake(t, &fakeDynamo{
		put: func(in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			id := string(in.Item[attrKey].(*types.AttributeValueMemberB).Value)
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
		del: func(in *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
			id := string(in.Key[attrKey].(*types.AttributeValueMemberB).Value)
			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, putTokens[id], string(in.ExpressionAttributeValues[":tk"].(*types.AttributeValueMemberB).Value), "released by the token it was put with")
			released = append(released, id[len(id)-3:])
			return &dynamodb.DeleteItemOutput{}, nil
		},
	})
	_, err := m.Reserve(t.Context(), keys("ok1", "dup", "bad", "ok2"), time.Minute)
	require.ErrorIs(t, err, ErrUnavailable)
	assert.ElementsMatch(t, []string{"ok1", "bad", "ok2"}, released, "the failed put may have landed; the duplicate was never ours")
}

func TestDynamo_CommitRetriesUnprocessedItems(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	var mu sync.Mutex
	written := map[string]int{}
	heldBack := map[string]bool{}
	_, m := openFake(t, &fakeDynamo{batch: func(in *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
		calls.Add(1)
		reqs := in.RequestItems["dedupe"]
		assert.LessOrEqual(t, len(reqs), batchWriteMax)
		// Leave the last item of every call unprocessed once.
		mu.Lock()
		defer mu.Unlock()
		var left []types.WriteRequest
		for i, r := range reqs {
			pk := string(r.PutRequest.Item[attrKey].(*types.AttributeValueMemberB).Value)
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
	_, m := openFake(t, &fakeDynamo{batch: func(in *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
		return &dynamodb.BatchWriteItemOutput{UnprocessedItems: in.RequestItems}, nil
	}})
	err := m.Commit(t.Context(), []Claim{{Key: keys("a")[0], Status: Claimed, Token: "t"}}, 0)
	require.ErrorIs(t, err, ErrUnavailable)
}

func TestDynamo_ReleaseTreatsAFailedConditionAsDone(t *testing.T) {
	t.Parallel()
	_, m := openFake(t, &fakeDynamo{del: func(in *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
		assert.Equal(t, condRelease, aws.ToString(in.ConditionExpression))
		id := string(in.Key[attrKey].(*types.AttributeValueMemberB).Value)
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
	d, m := openFake(t, &fakeDynamo{put: func(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
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
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeB}},
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

func TestExpiresAt(t *testing.T) {
	t.Parallel()
	base := time.Unix(100, 0)
	assert.Equal(t, int64(101), expiresAt(base, time.Second))
	assert.Equal(t, int64(102), expiresAt(base, 1500*time.Millisecond), "rounded up: never ends early")
	assert.Equal(t, int64(102), expiresAt(base.Add(time.Nanosecond), time.Second))
}

//go:build integration

package tests

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/dedupe"
	"github.com/Wave-RF/WaveHouse/internal/dedupe/dedupetest"
)

var dynamoTables atomic.Uint64

// newDynamoTable names a fresh table on dynamodb-local for one test.
func newDynamoTable() string {
	return fmt.Sprintf("dedupe_%d", dynamoTables.Add(1))
}

// dynamoClient is one client — one pod's view — over table on
// dynamodb-local, through the production constructor.
func dynamoClient(t *testing.T, table string, cfg dedupe.DynamoConfig, extra ...func(*config.LoadOptions) error) *dedupe.Dynamo {
	t.Helper()
	cfg.Table, cfg.Endpoint, cfg.Region = table, env(t).dynamoEndpoint, "us-east-1"
	if cfg.Timeout == 0 {
		// dynamodb-local under a parallel suite is slower than the real thing.
		cfg.Timeout = 5 * time.Second
	}
	opts := append([]func(*config.LoadOptions) error{
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")),
	}, extra...)
	d, err := dedupe.NewDynamo(t.Context(), cfg, opts...)
	require.NoError(t, err)
	return d
}

// rawDynamo is a plain client, for reading and planting items directly.
func rawDynamo(t *testing.T) *dynamodb.Client {
	t.Helper()
	return dynamodb.New(dynamodb.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(env(t).dynamoEndpoint),
		Credentials:  credentials.NewStaticCredentialsProvider("local", "local", ""),
	})
}

// faultyHTTP answers matching requests itself instead of sending them.
type faultyHTTP struct {
	next  *http.Client
	fault func(target string) (*http.Response, error, bool)
}

func (f *faultyHTTP) Do(r *http.Request) (*http.Response, error) {
	if resp, err, ok := f.fault(r.Header.Get("X-Amz-Target")); ok {
		return resp, err
	}
	return f.next.Do(r)
}

// awsError is a DynamoDB JSON error response.
func awsError(status int, code string) *http.Response {
	body := fmt.Sprintf(`{"__type":"com.amazonaws.dynamodb.v20120810#%s","message":"injected"}`, code)
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": {"application/x-amz-json-1.0"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

const putItem = "DynamoDB_20120810.PutItem"

// landThenFail sends the first PutItem, then answers it 500 as if the
// response were lost: the write is applied and the SDK retries it.
type landThenFail struct {
	next   *http.Client
	failed atomic.Bool
}

func (l *landThenFail) Do(r *http.Request) (*http.Response, error) {
	resp, err := l.next.Do(r)
	if err != nil || r.Header.Get("X-Amz-Target") != putItem || !l.failed.CompareAndSwap(false, true) {
		return resp, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return awsError(http.StatusInternalServerError, "InternalServerError"), nil
}

func TestDedupeDynamo_Conformance(t *testing.T) {
	t.Parallel()
	dedupetest.Run(t, func(t *testing.T) dedupetest.Harness {
		table := newDynamoTable()
		// failAfter < 0 is off; otherwise the put after that many fails once.
		var failAfter, puts atomic.Int64
		failAfter.Store(-1)
		fault := config.WithHTTPClient(&faultyHTTP{next: http.DefaultClient, fault: func(target string) (*http.Response, error, bool) {
			if target != putItem || failAfter.Load() < 0 || puts.Add(1) <= failAfter.Load() {
				return nil, nil, false
			}
			failAfter.Store(-1)
			return awsError(http.StatusBadRequest, "ValidationException"), nil, true
		}})
		d := dynamoClient(t, table, dedupe.DynamoConfig{}, fault)
		require.NoError(t, d.CreateTable(t.Context()))
		require.NoError(t, d.Check(t.Context()))
		return dedupetest.Harness{
			Factory: d.Tenant,
			Peer:    dynamoClient(t, table, dedupe.DynamoConfig{}).Tenant,
			FailNextReserve: func(n int) {
				puts.Store(0)
				failAfter.Store(int64(n))
			},
		}
	})
}

// 32 clients — 32 pods — race one id: DynamoDB's condition, not anything in
// process, is what lets exactly one through.
func TestDedupeDynamo_ThirtyTwoClientsOneID(t *testing.T) {
	t.Parallel()
	table := newDynamoTable()
	first := dynamoClient(t, table, dedupe.DynamoConfig{})
	require.NoError(t, first.CreateTable(t.Context()))
	const n = 32
	stores := make([]*dedupe.Managed, n)
	for i := range stores {
		stores[i] = dynamoClient(t, table, dedupe.DynamoConfig{}).Tenant("acme")
		require.NoError(t, stores[i].Apply(true))
	}
	k := []dedupe.Key{{Table: "events", ID: "e1"}}
	race := func() map[dedupe.Status][]dedupe.Claim {
		got := make([]dedupe.Claim, n)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i, s := range stores {
			wg.Go(func() {
				<-start
				c, err := s.Reserve(context.Background(), k, time.Minute)
				if assert.NoError(t, err) {
					got[i] = c[0]
				}
			})
		}
		close(start)
		wg.Wait()
		by := map[dedupe.Status][]dedupe.Claim{}
		for _, c := range got {
			by[c.Status] = append(by[c.Status], c)
		}
		return by
	}
	by := race()
	require.Len(t, by[dedupe.Claimed], 1, "exactly one client claims the id")
	assert.Len(t, by[dedupe.InFlight], n-1)
	require.NoError(t, stores[0].Commit(t.Context(), by[dedupe.Claimed], 0))
	assert.Len(t, race()[dedupe.Duplicate], n, "and every client then sees it committed")
}

func TestDedupeDynamo_Throttled(t *testing.T) {
	t.Parallel()
	table := newDynamoTable()
	require.NoError(t, dynamoClient(t, table, dedupe.DynamoConfig{}).CreateTable(t.Context()))
	for _, code := range []string{"ThrottlingException", "ProvisionedThroughputExceededException", "RequestLimitExceeded"} {
		t.Run(code, func(t *testing.T) {
			t.Parallel()
			var sent atomic.Int64
			d := dynamoClient(t, table, dedupe.DynamoConfig{MaxAttempts: 2}, config.WithHTTPClient(&faultyHTTP{
				next: http.DefaultClient,
				fault: func(target string) (*http.Response, error, bool) {
					if target != putItem {
						return nil, nil, false
					}
					sent.Add(1)
					return awsError(http.StatusBadRequest, code), nil, true
				},
			}))
			m := d.Tenant("acme")
			require.NoError(t, m.Apply(true))
			_, err := m.Reserve(t.Context(), []dedupe.Key{{Table: "events", ID: "e1"}}, time.Minute)
			require.ErrorIs(t, err, dedupe.ErrUnavailable, "a throttle is worth retrying")
			assert.Equal(t, int64(2), sent.Load(), "the SDK retried it once first")
		})
	}
}

// The SDK's retry of an applied put fails its condition on the put's own
// item, which is still the caller's claim: without that, the id would be
// held InFlight for the lease by a claim nobody commits or releases.
func TestDedupeDynamo_RetriedPutKeepsItsClaim(t *testing.T) {
	t.Parallel()
	table := newDynamoTable()
	lossy := &landThenFail{next: http.DefaultClient}
	d := dynamoClient(t, table, dedupe.DynamoConfig{}, config.WithHTTPClient(lossy))
	require.NoError(t, d.CreateTable(t.Context()))
	m := d.Tenant("acme")
	require.NoError(t, m.Apply(true))
	peer := dynamoClient(t, table, dedupe.DynamoConfig{}).Tenant("acme")
	require.NoError(t, peer.Apply(true))
	k := []dedupe.Key{{Table: "events", ID: "e1"}}

	claims, err := m.Reserve(t.Context(), k, time.Minute)
	require.NoError(t, err)
	require.True(t, lossy.failed.Load(), "the applied attempt was answered 500")
	require.Equal(t, dedupe.Claimed, claims[0].Status)
	other, err := peer.Reserve(t.Context(), k, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, dedupe.InFlight, other[0].Status)

	require.NoError(t, m.Release(t.Context(), claims))
	other, err = peer.Reserve(t.Context(), k, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, dedupe.Claimed, other[0].Status, "the claim's token was the applied put's, so Release freed the id")
}

func TestDedupeDynamo_Unreachable(t *testing.T) {
	t.Parallel()
	d, err := dedupe.NewDynamo(t.Context(), dedupe.DynamoConfig{
		Table: "dedupe", Region: "us-east-1", Endpoint: "http://127.0.0.1:1", MaxAttempts: 1,
	}, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")))
	require.NoError(t, err)
	m := d.Tenant("acme")
	require.NoError(t, m.Apply(true))
	k := []dedupe.Key{{Table: "events", ID: "e1"}}
	for range 5 {
		_, err = m.Reserve(t.Context(), k, time.Minute)
		require.ErrorIs(t, err, dedupe.ErrUnavailable)
	}
	_, err = m.Reserve(t.Context(), k, time.Minute)
	require.ErrorIs(t, err, dedupe.ErrUnavailable)
	assert.Contains(t, err.Error(), "short-circuited", "five failures in a second open the breaker")
	assert.ErrorIs(t, d.Check(t.Context()), dedupe.ErrUnavailable)
}

func TestDedupeDynamo_ConfigErrorsAreNotUnavailable(t *testing.T) {
	t.Parallel()
	d := dynamoClient(t, "no_such_table", dedupe.DynamoConfig{})
	m := d.Tenant("acme")
	require.NoError(t, m.Apply(true))
	_, err := m.Reserve(t.Context(), []dedupe.Key{{Table: "events", ID: "e1"}}, time.Minute)
	require.Error(t, err)
	var missing *types.ResourceNotFoundException
	assert.ErrorAs(t, err, &missing)
	assert.False(t, errors.Is(err, dedupe.ErrUnavailable), "a missing table is a config bug, not worth retrying")
	require.Error(t, d.Check(t.Context()))
}

func TestDedupeDynamo_Check(t *testing.T) {
	t.Parallel()
	raw := rawDynamo(t)
	table := newDynamoTable()
	_, err := raw.CreateTable(t.Context(), &dynamodb.CreateTableInput{
		TableName:            aws.String(table),
		BillingMode:          types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeB}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
	})
	require.NoError(t, err)
	assert.ErrorContains(t, dynamoClient(t, table, dedupe.DynamoConfig{}).Check(t.Context()), "must be a string")

	fresh := newDynamoTable()
	d := dynamoClient(t, fresh, dedupe.DynamoConfig{})
	require.NoError(t, d.CreateTable(t.Context()))
	require.NoError(t, d.CreateTable(t.Context()), "an existing table is left alone")
	ttl, err := raw.DescribeTimeToLive(t.Context(), &dynamodb.DescribeTimeToLiveInput{TableName: aws.String(fresh)})
	require.NoError(t, err)
	assert.Equal(t, "ex", aws.ToString(ttl.TimeToLiveDescription.AttributeName))
	assert.Equal(t, types.TimeToLiveStatusEnabled, ttl.TimeToLiveDescription.TimeToLiveStatus)
}

// Expiry is the item's ex, in epoch seconds, and never depends on TTL having
// deleted the item.
func TestDedupeDynamo_Expiry(t *testing.T) {
	t.Parallel()
	raw := rawDynamo(t)
	table := newDynamoTable()
	d := dynamoClient(t, table, dedupe.DynamoConfig{})
	require.NoError(t, d.CreateTable(t.Context()))
	m := d.Tenant("acme")
	require.NoError(t, m.Apply(true))
	pk := func(id string) string {
		return string(dedupe.AppendKey(nil, dedupe.KeyPrefix("acme"), dedupe.Key{Table: "events", ID: id}))
	}
	item := func(id string) map[string]types.AttributeValue {
		out, err := raw.GetItem(t.Context(), &dynamodb.GetItemInput{
			TableName: aws.String(table), ConsistentRead: aws.Bool(true),
			Key: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: pk(id)}},
		})
		require.NoError(t, err)
		return out.Item
	}
	num := func(av types.AttributeValue) int64 {
		n, err := strconv.ParseInt(av.(*types.AttributeValueMemberN).Value, 10, 64)
		require.NoError(t, err)
		return n
	}

	before := time.Now()
	claims, err := m.Reserve(t.Context(), []dedupe.Key{{Table: "events", ID: "kept"}, {Table: "events", ID: "brief"}}, 30*time.Second)
	require.NoError(t, err)
	pending := item("kept")
	assert.Equal(t, "1", pending["st"].(*types.AttributeValueMemberN).Value)
	assert.InDelta(t, before.Add(30*time.Second).Unix(), num(pending["ex"]), 2, "a pending item's ex is its lease end")

	require.NoError(t, m.Commit(t.Context(), claims[:1], 0))
	require.NoError(t, m.Commit(t.Context(), claims[1:], time.Hour))
	assert.NotContains(t, item("kept"), "ex", "retention 0 writes no ex, so TTL never takes it")
	assert.InDelta(t, before.Add(time.Hour).Unix(), num(item("brief")["ex"]), 2, "a commit's ex is its retention end")

	// TTL deletes lazily; an item whose ex has passed is absent all the same.
	for _, st := range []string{"1", "2"} {
		_, err = raw.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: aws.String(table), Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk("stale-" + st)},
			"st": &types.AttributeValueMemberN{Value: st},
			"ex": &types.AttributeValueMemberN{Value: strconv.FormatInt(time.Now().Add(-time.Minute).Unix(), 10)},
			"tk": &types.AttributeValueMemberB{Value: []byte("old")},
		}})
		require.NoError(t, err)
	}
	got, err := m.Reserve(t.Context(), []dedupe.Key{{Table: "events", ID: "stale-1"}, {Table: "events", ID: "stale-2"}, {Table: "events", ID: "kept"}}, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, []dedupe.Status{dedupe.Claimed, dedupe.Claimed, dedupe.Duplicate},
		[]dedupe.Status{got[0].Status, got[1].Status, got[2].Status})
}

func TestDedupeDynamo_CreateTableNeedsEndpoint(t *testing.T) {
	t.Parallel()
	d, err := dedupe.NewDynamo(t.Context(), dedupe.DynamoConfig{Table: "dedupe", Region: "us-east-1"},
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")))
	require.NoError(t, err)
	require.ErrorIs(t, d.CreateTable(t.Context()), dedupe.ErrCreateTableNeedsEndpoint)
}

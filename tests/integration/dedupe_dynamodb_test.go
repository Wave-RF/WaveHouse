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
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
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

// createPKTable creates table keyed by pk alone, the layout CreateTable made
// before pk+sk, with TTL on ex: a table an earlier deployment created.
func createPKTable(t *testing.T, table string) {
	t.Helper()
	raw := rawDynamo(t)
	_, err := raw.CreateTable(t.Context(), &dynamodb.CreateTableInput{
		TableName:            aws.String(table),
		BillingMode:          types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
	})
	require.NoError(t, err)
	_, err = raw.UpdateTimeToLive(t.Context(), &dynamodb.UpdateTimeToLiveInput{
		TableName:               aws.String(table),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{AttributeName: aws.String("ex"), Enabled: aws.Bool(true)},
	})
	require.NoError(t, err)
}

// dynamoLayouts are the two key schemas the backend serves, each with what
// creates a table of it and the item key it gives tenant tn's k.
var dynamoLayouts = []struct {
	name   string
	create func(t *testing.T, table string)
	key    func(tn tenant.ID, k dedupe.Key) map[string]types.AttributeValue
}{
	{
		name:   "pk",
		create: createPKTable,
		key: func(tn tenant.ID, k dedupe.Key) map[string]types.AttributeValue {
			return map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: string(dedupe.AppendKey(nil, dedupe.KeyPrefix(tn), k))}}
		},
	},
	{
		name: "pk+sk",
		create: func(t *testing.T, table string) {
			t.Helper()
			require.NoError(t, dynamoClient(t, table, dedupe.DynamoConfig{}).CreateTable(t.Context()))
		},
		key: func(tn tenant.ID, k dedupe.Key) map[string]types.AttributeValue {
			pk, sk := dedupe.PartitionedKey(tn, k)
			return map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: pk}, "sk": &types.AttributeValueMemberS{Value: sk}}
		},
	},
}

// openDynamo checks d's table, which every store's open waits for, and opens
// tenant id's store through d.
func openDynamo(t *testing.T, d *dedupe.Dynamo, id tenant.ID) *dedupe.Managed {
	t.Helper()
	require.NoError(t, d.Check(t.Context()))
	m := d.Tenant(id)
	require.NoError(t, m.Apply(true))
	return m
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
	return f.next.Do(r) //nolint:gosec // G704: forwards the AWS SDK's own request to the local dynamodb
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
	resp, err := l.next.Do(r) //nolint:gosec // G704: forwards the AWS SDK's own request to the local dynamodb
	if err != nil || r.Header.Get("X-Amz-Target") != putItem || !l.failed.CompareAndSwap(false, true) {
		return resp, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return awsError(http.StatusInternalServerError, "InternalServerError"), nil
}

// Both layouts keep the same contract: only the item key differs.
func TestDedupeDynamo_Conformance(t *testing.T) {
	t.Parallel()
	for _, l := range dynamoLayouts {
		t.Run(l.name, func(t *testing.T) {
			t.Parallel()
			dedupetest.Run(t, func(t *testing.T) dedupetest.Harness {
				table := newDynamoTable()
				l.create(t, table)
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
				require.NoError(t, d.Check(t.Context()))
				peer := dynamoClient(t, table, dedupe.DynamoConfig{})
				require.NoError(t, peer.Check(t.Context()))
				return dedupetest.Harness{
					Factory: d.Tenant,
					Peer:    peer.Tenant,
					FailNextReserve: func(n int) {
						puts.Store(0)
						failAfter.Store(int64(n))
					},
				}
			})
		})
	}
}

// On a pk+sk table a tenant's items are its own TenantBuckets partitions and
// nothing else: a Query of acme's finds every id acme committed, and none of
// acme-2's or acme_x's, though those tenants committed the same ids.
func TestDedupeDynamo_TenantPartitions(t *testing.T) {
	t.Parallel()
	table := newDynamoTable()
	d := dynamoClient(t, table, dedupe.DynamoConfig{})
	require.NoError(t, d.CreateTable(t.Context()))
	var want []string
	for i := range 64 {
		want = append(want, fmt.Sprintf("events/e%d", i))
	}
	for _, tn := range []tenant.ID{"acme", "acme-2", "acme_x"} {
		m := openDynamo(t, d, tn)
		for i := range 64 {
			dup, err := dedupetest.Mark(t.Context(), m, dedupe.Key{Table: "events", ID: fmt.Sprintf("e%d", i)})
			require.NoError(t, err)
			require.False(t, dup)
		}
	}
	raw := rawDynamo(t)
	query := func(tn tenant.ID) (sks []string) {
		for b := range dedupe.TenantBuckets {
			pk := fmt.Sprintf("%s/%d", tn, b)
			pages := dynamodb.NewQueryPaginator(raw, &dynamodb.QueryInput{
				TableName:                 aws.String(table),
				ConsistentRead:            aws.Bool(true),
				KeyConditionExpression:    aws.String("pk = :pk"),
				ExpressionAttributeValues: map[string]types.AttributeValue{":pk": &types.AttributeValueMemberS{Value: pk}},
			})
			for pages.HasMorePages() {
				page, err := pages.NextPage(t.Context())
				require.NoError(t, err)
				for _, item := range page.Items {
					require.Equal(t, pk, item["pk"].(*types.AttributeValueMemberS).Value)
					sks = append(sks, item["sk"].(*types.AttributeValueMemberS).Value)
				}
			}
		}
		return sks
	}
	assert.ElementsMatch(t, want, query("acme"))
	assert.ElementsMatch(t, want, query("acme-2"), "the other tenants' items are there, under their own keys")
	scan, err := raw.Scan(t.Context(), &dynamodb.ScanInput{TableName: aws.String(table), Select: types.SelectCount})
	require.NoError(t, err)
	assert.Equal(t, int32(3*64), scan.Count)
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
		stores[i] = openDynamo(t, dynamoClient(t, table, dedupe.DynamoConfig{}), "acme")
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
			m := openDynamo(t, d, "acme")
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
	m := openDynamo(t, d, "acme")
	peer := openDynamo(t, dynamoClient(t, table, dedupe.DynamoConfig{}), "acme")
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

// outage sends requests on to dynamodb-local until down, then to a port
// nothing listens on: a client that passed its check, then lost the table.
type outage struct{ down atomic.Bool }

func (o *outage) Do(r *http.Request) (*http.Response, error) {
	if o.down.Load() {
		r = r.Clone(r.Context())
		r.URL.Host = "127.0.0.1:1"
	}
	return http.DefaultClient.Do(r) //nolint:gosec // G704: forwards the AWS SDK's own request to the local dynamodb, or nowhere
}

func TestDedupeDynamo_Unreachable(t *testing.T) {
	t.Parallel()
	const window = time.Second // the breaker opens on five failures inside it
	k := []dedupe.Key{{Table: "events", ID: "e1"}}
	table := newDynamoTable()
	require.NoError(t, dynamoClient(t, table, dedupe.DynamoConfig{}).CreateTable(t.Context()))
	var d *dedupe.Dynamo
	// Each attempt is a fresh client, so the breaker's count never carries
	// over. Four concurrent failures must not open it, the fifth must, and
	// the sixth call must be refused without a dial. An attempt that took
	// under half the window cannot have missed it, so only a slower one (a
	// stalled host) may be tried again.
	for attempt := 1; ; attempt++ {
		link := &outage{}
		d = dynamoClient(t, table, dedupe.DynamoConfig{MaxAttempts: 1}, config.WithHTTPClient(link))
		m := openDynamo(t, d, "acme")
		link.down.Store(true)

		reserve := func() error {
			_, err := m.Reserve(t.Context(), k, time.Minute)
			require.ErrorIs(t, err, dedupe.ErrUnavailable)
			return err
		}
		start := time.Now()
		var wg sync.WaitGroup
		errs := make([]error, 4)
		for i := range errs {
			wg.Go(func() { _, errs[i] = m.Reserve(t.Context(), k, time.Minute) })
		}
		wg.Wait()
		for _, err := range errs {
			require.ErrorIs(t, err, dedupe.ErrUnavailable)
			require.NotContains(t, err.Error(), "short-circuited", "the breaker opened before five failures")
		}
		fifth := reserve()
		require.NotContains(t, fifth.Error(), "short-circuited", "the breaker opened before five failures")
		sixth := reserve()
		took := time.Since(start)
		if strings.Contains(sixth.Error(), "short-circuited") {
			break
		}
		require.Greater(t, took, window/2, "five failures in under half a second did not open the breaker")
		require.Less(t, attempt, 5, "five failures in a second never opened the breaker")
		t.Logf("attempt %d: the six calls took %s, outside the breaker's window; trying again", attempt, took)
	}
	assert.ErrorIs(t, d.Check(t.Context()), dedupe.ErrUnavailable)
}

// A pk+sk table replaced under the same name by one keyed by pk alone, under
// a running store: the store fails closed instead of reading the item another
// id in its bucket left as this id's.
func TestDedupeDynamo_LostSortKeyFailsClosed(t *testing.T) {
	t.Parallel()
	table := newDynamoTable()
	d := dynamoClient(t, table, dedupe.DynamoConfig{})
	require.NoError(t, d.CreateTable(t.Context()))
	m := openDynamo(t, d, "acme")
	_, err := rawDynamo(t).DeleteTable(t.Context(), &dynamodb.DeleteTableInput{TableName: aws.String(table)})
	require.NoError(t, err)
	createPKTable(t, table)

	first := dedupe.Key{Table: "events", ID: "e0"}
	bucket, _ := dedupe.PartitionedKey("acme", first)
	second := first
	for i := 1; ; i++ {
		second.ID = fmt.Sprintf("e%d", i)
		if pk, _ := dedupe.PartitionedKey("acme", second); pk == bucket {
			break
		}
	}
	dup, err := dedupetest.Mark(t.Context(), m, first)
	require.NoError(t, err)
	require.False(t, dup)
	_, err = m.Reserve(t.Context(), []dedupe.Key{second}, time.Minute)
	require.ErrorContains(t, err, "lost its sort key")
	assert.False(t, errors.Is(err, dedupe.ErrUnavailable), "a configuration error, not worth retrying")
}

// A table deleted under a running process is a configuration bug, not worth
// retrying.
func TestDedupeDynamo_ConfigErrorsAreNotUnavailable(t *testing.T) {
	t.Parallel()
	table := newDynamoTable()
	d := dynamoClient(t, table, dedupe.DynamoConfig{})
	require.NoError(t, d.CreateTable(t.Context()))
	m := openDynamo(t, d, "acme")
	_, err := rawDynamo(t).DeleteTable(t.Context(), &dynamodb.DeleteTableInput{TableName: aws.String(table)})
	require.NoError(t, err)
	_, err = m.Reserve(t.Context(), []dedupe.Key{{Table: "events", ID: "e1"}}, time.Minute)
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

	other := newDynamoTable()
	_, err = raw.CreateTable(t.Context(), &dynamodb.CreateTableInput{
		TableName:   aws.String(other),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeRange},
		},
	})
	require.NoError(t, err)
	assert.ErrorContains(t, dynamoClient(t, other, dedupe.DynamoConfig{}).Check(t.Context()), "key schema")

	pkOnly := newDynamoTable()
	createPKTable(t, pkOnly)
	require.NoError(t, dynamoClient(t, pkOnly, dedupe.DynamoConfig{}).Check(t.Context()), "a table keyed by pk alone is still served")

	fresh := newDynamoTable()
	d := dynamoClient(t, fresh, dedupe.DynamoConfig{})
	require.NoError(t, d.CreateTable(t.Context()))
	require.NoError(t, d.CreateTable(t.Context()), "an existing table is left alone")
	require.NoError(t, d.Check(t.Context()))
	desc, err := raw.DescribeTable(t.Context(), &dynamodb.DescribeTableInput{TableName: aws.String(fresh)})
	require.NoError(t, err)
	keys := map[string]types.KeyType{}
	for _, k := range desc.Table.KeySchema {
		keys[aws.ToString(k.AttributeName)] = k.KeyType
	}
	assert.Equal(t, map[string]types.KeyType{"pk": types.KeyTypeHash, "sk": types.KeyTypeRange}, keys, "CreateTable makes a pk+sk table")
	ttl, err := raw.DescribeTimeToLive(t.Context(), &dynamodb.DescribeTimeToLiveInput{TableName: aws.String(fresh)})
	require.NoError(t, err)
	assert.Equal(t, "ex", aws.ToString(ttl.TimeToLiveDescription.AttributeName))
	assert.Equal(t, types.TimeToLiveStatusEnabled, ttl.TimeToLiveDescription.TimeToLiveStatus)
}

// Expiry is the item's ex, in epoch seconds, and never depends on TTL having
// deleted the item. Each layout's item sits at the key that layout names, so
// a pk table's items are where they always were.
func TestDedupeDynamo_Expiry(t *testing.T) {
	t.Parallel()
	for _, l := range dynamoLayouts {
		t.Run(l.name, func(t *testing.T) {
			t.Parallel()
			raw := rawDynamo(t)
			table := newDynamoTable()
			l.create(t, table)
			m := openDynamo(t, dynamoClient(t, table, dedupe.DynamoConfig{}), "acme")
			key := func(id string) map[string]types.AttributeValue {
				return l.key("acme", dedupe.Key{Table: "events", ID: id})
			}
			item := func(id string) map[string]types.AttributeValue {
				out, err := raw.GetItem(t.Context(), &dynamodb.GetItemInput{
					TableName: aws.String(table), ConsistentRead: aws.Bool(true), Key: key(id),
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
			require.NotEmpty(t, pending, "the claim is at its layout's key")
			assert.Equal(t, "1", pending["st"].(*types.AttributeValueMemberN).Value)
			assert.InDelta(t, before.Add(30*time.Second).Unix(), num(pending["ex"]), 2, "a pending item's ex is its lease end")

			require.NoError(t, m.Commit(t.Context(), claims[:1], 0))
			require.NoError(t, m.Commit(t.Context(), claims[1:], time.Hour))
			assert.NotContains(t, item("kept"), "ex", "retention 0 writes no ex, so TTL never takes it")
			assert.InDelta(t, before.Add(time.Hour).Unix(), num(item("brief")["ex"]), 2, "a commit's ex is its retention end")

			// TTL deletes lazily; an item whose ex has passed is absent all the same.
			for _, st := range []string{"1", "2"} {
				stale := key("stale-" + st)
				stale["st"] = &types.AttributeValueMemberN{Value: st}
				stale["ex"] = &types.AttributeValueMemberN{Value: strconv.FormatInt(time.Now().Add(-time.Minute).Unix(), 10)}
				stale["tk"] = &types.AttributeValueMemberB{Value: []byte("old")}
				_, err = raw.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: aws.String(table), Item: stale})
				require.NoError(t, err)
			}
			got, err := m.Reserve(t.Context(), []dedupe.Key{{Table: "events", ID: "stale-1"}, {Table: "events", ID: "stale-2"}, {Table: "events", ID: "kept"}}, time.Minute)
			require.NoError(t, err)
			assert.Equal(t, []dedupe.Status{dedupe.Claimed, dedupe.Claimed, dedupe.Duplicate},
				[]dedupe.Status{got[0].Status, got[1].Status, got[2].Status})
		})
	}
}

func TestDedupeDynamo_CreateTableNeedsEndpoint(t *testing.T) {
	t.Parallel()
	d, err := dedupe.NewDynamo(t.Context(), dedupe.DynamoConfig{Table: "dedupe", Region: "us-east-1"},
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")))
	require.NoError(t, err)
	require.ErrorIs(t, d.CreateTable(t.Context()), dedupe.ErrCreateTableNeedsEndpoint)
}

package pipes

import (
	"context"
	"os"
	"path/filepath"
    "encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBindParams_AllSupplied(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL: "SELECT * FROM clicks WHERE page = {{page}} AND count > {{min_count}}",
		Parameters: []ParamDef{
			{Name: "page", Type: "string", Required: true},
			{Name: "min_count", Type: "number", Required: true},
		},
	}
	sql, params, err := BindParams(q, map[string]any{"page": "/home", "min_count": 10})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM clicks WHERE page = '/home' AND count > 10", sql)
	assert.Nil(t, params)
}

func TestBindParams_MissingRequired(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM clicks WHERE page = {{page}}",
		Parameters: []ParamDef{{Name: "page", Type: "string", Required: true}},
	}
	_, _, err := BindParams(q, map[string]any{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing required parameter: page")
}

func TestBindParams_DefaultApplied(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM clicks LIMIT {{limit}}",
		Parameters: []ParamDef{{Name: "limit", Type: "number", Default: 100}},
	}
	sql, params, err := BindParams(q, map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM clicks LIMIT 100", sql)
	assert.Nil(t, params)
}

func TestBindParams_MultipleOccurrences(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM t WHERE a = {{val}} OR b = {{val}}",
		Parameters: []ParamDef{{Name: "val", Type: "string", Required: true}},
	}
	sql, params, err := BindParams(q, map[string]any{"val": "x"})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE a = 'x' OR b = 'x'", sql)
	assert.Nil(t, params)
}

func TestBindParams_NoParameters(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{SQL: "SELECT count(*) FROM clicks"}
	sql, params, err := BindParams(q, map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, "SELECT count(*) FROM clicks", sql)
	assert.Empty(t, params)
}

func TestBindParams_OptionalWithDefault_Supplied(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM clicks LIMIT {{limit}}",
		Parameters: []ParamDef{{Name: "limit", Type: "number", Default: 100}},
	}
	sql, params, err := BindParams(q, map[string]any{"limit": 50})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM clicks LIMIT 50", sql)
	assert.Nil(t, params)
}

func TestBindParams_PlaceholderNotInSQL(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM clicks",
		Parameters: []ParamDef{{Name: "unused", Type: "string"}},
	}
	sql, params, err := BindParams(q, map[string]any{"unused": "val"})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM clicks", sql)
	assert.Empty(t, params, "unused param should not generate positional args")
}

func TestBindParams_InlineDefault_NoFormalParam(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL: "SELECT page, count() FROM clicks GROUP BY page LIMIT {{limit:10}}",
	}
	sql, params, err := BindParams(q, map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, "SELECT page, count() FROM clicks GROUP BY page LIMIT 10", sql)
	assert.Nil(t, params)
}

func TestBindParams_InlineDefault_SuppliedOverrides(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL: "SELECT * FROM clicks LIMIT {{limit:10}}",
	}
	sql, params, err := BindParams(q, map[string]any{"limit": float64(5)})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM clicks LIMIT 5", sql)
	assert.Nil(t, params)
}

func TestBindParams_InlineNoDefault_MissingRequired(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL: "SELECT * FROM clicks WHERE page = {{page}}",
	}
	_, _, err := BindParams(q, map[string]any{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing required parameter: page")
}

func TestBindParams_InlineMultipleParams(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL: "SELECT * FROM clicks WHERE country = {{country:US}} LIMIT {{limit:10}}",
	}
	sql, params, err := BindParams(q, map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM clicks WHERE country = 'US' LIMIT 10", sql)
	assert.Nil(t, params)
}

func TestBindParams_StringEscaping(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM t WHERE name = {{name}}",
		Parameters: []ParamDef{{Name: "name", Type: "string", Required: true}},
	}
	sql, _, err := BindParams(q, map[string]any{"name": "O'Brien"})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE name = 'O''Brien'", sql)
}

func TestBindParams_BooleanParam(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM t WHERE active = {{active}}",
		Parameters: []ParamDef{{Name: "active", Type: "boolean", Required: true}},
	}
	sql, _, err := BindParams(q, map[string]any{"active": true})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE active = 1", sql)
}

func TestBindParams_NilParam(t *testing.T) {
	t.Parallel()
	q := &NamedQuery{
		SQL:        "SELECT * FROM t WHERE col = {{val}}",
		Parameters: []ParamDef{{Name: "val", Type: "string", Default: nil}},
	}
	sql, _, err := BindParams(q, map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t WHERE col = NULL", sql)
}

func TestMemoryStore_GetListPut(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore(
		&NamedQuery{Name: "q1", SQL: "SELECT 1"},
		&NamedQuery{Name: "q2", SQL: "SELECT 2"},
	)

	// Get.
	q := store.Get("q1")
	require.NotNil(t, q)
	assert.Equal(t, "SELECT 1", q.SQL)

	assert.Nil(t, store.Get("missing"))

	// List.
	all := store.List()
	assert.Len(t, all, 2)

	// Put (memory store doesn't have KV, so Put won't work without kv).
	// Verify cached state directly.
	store.mu.Lock()
	store.cached["q3"] = &NamedQuery{Name: "q3", SQL: "SELECT 3"}
	store.mu.Unlock()

	assert.NotNil(t, store.Get("q3"))
	assert.Len(t, store.List(), 3)
}

func TestMemoryStore_Empty(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	assert.Empty(t, store.List())
	assert.Nil(t, store.Get("anything"))
}

func TestStore_Put_ValidatesRequiredFields(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	ctx := context.Background()

	err := store.Put(ctx, &NamedQuery{SQL: "SELECT 1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "name is required")

	err = store.Put(ctx, &NamedQuery{Name: "only_name"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SQL is required")
}

func TestStore_Put_CachesWithoutKV(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	ctx := context.Background()

	q := &NamedQuery{Name: "count_clicks", SQL: "SELECT count() FROM clicks"}
	require.NoError(t, store.Put(ctx, q))

	got := store.Get("count_clicks")
	require.NotNil(t, got)
	assert.Equal(t, q.SQL, got.SQL)
}

func TestStore_Delete_RemovesFromCacheWithoutKV(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore(
		&NamedQuery{Name: "a", SQL: "SELECT 1"},
		&NamedQuery{Name: "b", SQL: "SELECT 2"},
	)
	ctx := context.Background()

	require.NoError(t, store.Delete(ctx, "a"))
	assert.Nil(t, store.Get("a"))
	assert.NotNil(t, store.Get("b"))
	assert.Len(t, store.List(), 1)
}

func TestStore_LoadFromDirectory_MissingDirIsOK(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	ctx := context.Background()
	// Non-existent directory returns nil (not an error).
	assert.NoError(t, store.loadFromDirectory(ctx, filepath.Join(t.TempDir(), "does-not-exist")))
}

func TestStore_LoadFromDirectory_EmptyDirReturnsNil(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	ctx := context.Background()
	// Empty directory: ReadDir succeeds, no entries to iterate. kv is never touched.
	assert.NoError(t, store.loadFromDirectory(ctx, t.TempDir()))

	// Directory with only non-.sql files: each is skipped before kv is touched.
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "notes.md"), []byte("ignored"), 0o600))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "sub"), 0o750))
	assert.NoError(t, store.loadFromDirectory(ctx, dir))
}

// -----------------------------------------------------------------------------
// Watcher Tests & NATS Mocks
// -----------------------------------------------------------------------------

// mockWatcher implements jetstream.KeyWatcher for testing
type mockWatcher struct {
	updates chan jetstream.KeyValueEntry
}

func (m *mockWatcher) Updates() <-chan jetstream.KeyValueEntry {
	return m.updates

}
func (m *mockWatcher) Stop() error {
	return nil
}

// mockKVEntry implements jetstream.KeyValueEntry for testing
type mockKVEntry struct {
	key   string
	val   []byte
	op    jetstream.KeyValueOp
	rev   uint64					// Keeps track of any updates to a pipe (version number)
	delta uint64					// Keeps track of how many more updates are waiting in the queue behind the current one
}

func (m *mockKVEntry) Bucket() string {
	return kvBucket
}

func (m *mockKVEntry) Key() string {
	return m.key
}

func (m *mockKVEntry) Value() []byte {
	return m.val
}

func (m *mockKVEntry) Revision() uint64 {
	return m.rev
}

func (m *mockKVEntry) Created() time.Time {
	return time.Now()
}

func (m *mockKVEntry) Delta() uint64 {
	return m.delta
}

func (m *mockKVEntry) Operation() jetstream.KeyValueOp {
	return m.op
}

// mockKV implements a subset of jetstream.KeyValue for testing
type mockKV struct {
	jetstream.KeyValue // embed to satisfy interface for unused methods
	watcher            *mockWatcher
}

func (m *mockKV) WatchAll(ctx context.Context, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	return m.watcher, nil
}

func TestStore_Watch_SyncsCluster(t *testing.T) {
	t.Parallel()
	
	// Setup the mock NATS watcher
	updatesCh := make(chan jetstream.KeyValueEntry)
	watcher := &mockWatcher{updates: updatesCh}
	kv := &mockKV{watcher: watcher}

	// Create a Store and inject our mock NATS KV
	store := NewMemoryStore()
	store.kv = kv // Override the nil kv with our mock

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the watcher in the background
	go store.Watch(ctx)

	// Yield briefly to let the background goroutine start
	time.Sleep(10 * time.Millisecond)

	// Simulate NATS sending a "Put" (A new Pipe was created)
	newPipe := NamedQuery{Name: "test_pipe", SQL: "SELECT 1"}
	data, err := json.Marshal(newPipe)
	require.NoError(t, err)

	updatesCh <- &mockKVEntry{
		key: "test_pipe",
		val: data,
		op:  jetstream.KeyValuePut,
	}

	// Wait for the background goroutine to process the channel
	time.Sleep(10 * time.Millisecond)

	// Verify the pipe is now in local memory
	cached := store.Get("test_pipe")
	require.NotNil(t, cached, "Pipe should have been added to cache by watcher")
	assert.Equal(t, "SELECT 1", cached.SQL)

	// Simulate NATS sending a "Delete" (A Pipe was removed)
	updatesCh <- &mockKVEntry{
		key: "test_pipe",
		op:  jetstream.KeyValueDelete,
	}

	time.Sleep(10 * time.Millisecond)

	// Verify the pipe was removed from local memory
	assert.Nil(t, store.Get("test_pipe"), "Pipe should have been removed from cache by watcher")
}

func TestStore_Watch_NilKV(t *testing.T) {
	t.Parallel()
	
	// If the Store is purely in-memory (no NATS configured), Watch should exit cleanly.
	store := NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// This would panic if the guard clause `if s.kv == nil` wasn't there
	store.Watch(ctx)
}

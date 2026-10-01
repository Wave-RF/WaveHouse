package typelayer

import (
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/wave-rf/chtypes/go/chtypes"
)

// maxPoolSize caps the identically-compiled handles a base table grows to.
//
// A LoadedSchema serializes its own calls, so one handle per table is a
// ceiling. Measured on a Linux arm64 VM (BenchmarkIngest_HandlePool, artifacts
// 26.6 and 25.8), own handles scale 4-6x at 8 goroutines, while a
// process-wide serialization gate never beats one thread (0.83-1.0x). A
// compiled handle costs ~40 KiB (~96 KiB warm). That cost multiplies by
// tables and by tenants, so a pool starts at one handle and grows only when
// every handle it has is busy (see pool.acquire). An earlier darwin run showed
// a flat curve, so treat the size as hardware-dependent and re-measure it on
// the deployment hardware.
const maxPoolSize = 8

// maxRolePoolSize caps a role-shape table's pool. Every insert of a
// column-restricted or check-injecting role runs on its shape's handles, so
// one handle would serialize that role's whole ingest; but a table holds up to
// roleCacheSize shapes, and 256 x 8 warm handles would be ~200 MiB where
// 256 x 4 is half that. Like a base table, a shape grows past one handle only
// under contention, so a quiet shape keeps one.
const maxRolePoolSize = 4

// poolSize is how many identical handles one base-table shape may grow to.
func poolSize() int {
	return max(min(runtime.GOMAXPROCS(0), maxPoolSize), 1)
}

// rolePoolSize is how many identical handles one role shape may grow to.
func rolePoolSize() int {
	return max(min(runtime.GOMAXPROCS(0), maxRolePoolSize), 1)
}

// compileSettings is the fixed parsing profile every table handle is compiled
// with. allow_errors_ratio turns "first bad row ends the batch" into
// skip-and-continue so every record gets its own verdict; skip_unknown_fields=0
// makes an unknown field a real per-row rejection (ClickHouse code 117) instead
// of silent data loss. Neither is ever forwarded to the real INSERT.
//
// The type gates admit the column types ClickHouse refuses to create by
// default. Every table compiled here already exists on the server, so its
// CREATE passed these gates there; without them chtypes refuses every record
// of, say, a LowCardinality(UInt64) table with code 455, which the server
// would insert. A gate name the loaded line does not know fails the compile
// with code 115 (allow_experimental_qbit_type does on 25.8), so the list holds
// only names measured accepted on 25.8, 26.6 and 26.8.
var compileSettings = map[string]string{
	"input_format_allow_errors_ratio":  "1",
	"input_format_skip_unknown_fields": "0",

	"allow_suspicious_low_cardinality_types": "1",
	"allow_suspicious_fixed_string_types":    "1",
	"allow_suspicious_variant_types":         "1",
	"allow_experimental_json_type":           "1",
	"allow_experimental_variant_type":        "1",
	"allow_experimental_dynamic_type":        "1",
	"allow_experimental_time_time64_type":    "1",
	"allow_experimental_bfloat16_type":       "1",
	"allow_experimental_object_type":         "1",
	"allow_experimental_nlp_functions":       "1",
}

// schemaSlot is one compiled handle plus the filters compiled against it. A
// filter is bound to the schema it was compiled from — evaluating it against a
// block from another handle takes BOTH handles' locks — so each slot owns its
// own cache and one evaluation stays on one slot.
type schemaSlot struct {
	schema  *chtypes.LoadedSchema
	filters *filterCache
	// busy counts the calls (and parsed Rows) currently on this slot.
	busy atomic.Int32
}

// closeSlots tears handles down. Filters go before the schema, which the C
// layer requires, and the cache index is emptied so no freed pointer survives.
func closeSlots(slots []*schemaSlot) {
	for _, s := range slots {
		if s == nil {
			continue
		}
		if s.filters != nil {
			s.filters.closeAll()
		}
		if s.schema != nil {
			s.schema.Close()
		}
	}
}

// pool is one table shape's identically-compiled handles. It is compiled with
// one handle and grows lazily, up to its limit, when a call finds every handle
// busy. Every slot is compiled from the same declaration list, so the handles
// are interchangeable and a call may land on any of them.
//
// Lifetime: a pool is only used by callers holding its Table's read lock, and
// only closed under that Table's write lock or once nothing can reach it, so
// growth and close never overlap.
type pool struct {
	lib *chtypes.Library
	ddl string
	// limit is the most slots the pool will hold; lowered to the current size
	// if a growth compile is ever refused, so a refusal costs one compile.
	limit atomic.Int64
	// grow admits one growing caller at a time; the others share a busy slot
	// rather than wait for a compile.
	grow sync.Mutex
	// slots is replaced, never mutated, so readers need no lock.
	slots atomic.Pointer[[]*schemaSlot]
	next  atomic.Uint64
}

// newPool compiles a pool's first handle. A refusal is the whole shape's: the
// handles are interchangeable by construction, so there is no half-pool.
func newPool(lib *chtypes.Library, ddl string, limit int) (*pool, string) {
	limit = max(limit, 1)
	p := &pool{lib: lib, ddl: ddl}
	p.limit.Store(int64(limit))
	first, err := p.compile()
	if err != nil {
		return nil, describeCompileError(err)
	}
	p.slots.Store(&[]*schemaSlot{first})
	return p, ""
}

func (p *pool) compile() (*schemaSlot, error) {
	schema, err := p.lib.CompileDDL(p.ddl, chtypes.WithCompileSettings(compileSettings))
	if err != nil {
		return nil, err
	}
	return &schemaSlot{schema: schema, filters: newFilterCache(filterCacheSize)}, nil
}

// list is the current slots, oldest first.
func (p *pool) list() []*schemaSlot { return *p.slots.Load() }

// first is the slot every pool has: what column introspection reads.
func (p *pool) first() *schemaSlot { return p.list()[0] }

// acquire picks the slot one call (or one parsed Row) runs on and marks it
// busy until release. The lowest idle slot is taken first; when every slot is
// busy and the pool is below its limit, the caller compiles one more and
// takes it; a caller that cannot grow the pool shares a busy slot, whose
// handle serializes the two calls. Growing on contention rather than at Bind
// is what keeps a quiet table, of a quiet tenant, at one handle.
//
// The LOWEST idle slot rather than a rotating one, because a filter answers
// only on the slot it was compiled on: a serial run of calls — a tenant's
// stream evaluating every subscriber's filter, event after event — stays on
// one slot and compiles each filter once, where rotating would compile it on
// every slot and keep a copy in each slot's cache. Copies then grow with real
// contention rather than with the pool's size.
func (p *pool) acquire() *schemaSlot {
	slots := p.list()
	for _, s := range slots {
		if s.busy.CompareAndSwap(0, 1) {
			return s
		}
	}
	if int64(len(slots)) < p.limit.Load() && p.grow.TryLock() {
		s := p.growLocked()
		p.grow.Unlock()
		if s != nil {
			return s
		}
	}
	// Every slot busy and the pool full: share one, rotating so the sharing
	// spreads.
	s := slots[p.next.Add(1)%uint64(len(slots))]
	s.busy.Add(1)
	return s
}

// growLocked returns a slot already marked busy for the caller — one that
// became idle, or one more compiled — or nil when the pool is full or the
// compile is refused.
func (p *pool) growLocked() *schemaSlot {
	cur := p.list()
	// A slot freed (or grown) since the caller looked is cheaper than a compile.
	for _, s := range cur {
		if s.busy.CompareAndSwap(0, 1) {
			return s
		}
	}
	if int64(len(cur)) >= p.limit.Load() {
		return nil
	}
	s, err := p.compile()
	if err != nil {
		// The first handle compiled from this same list, so this is not a
		// verdict about the table: stop growing and keep serving on what the
		// pool has.
		p.limit.Store(int64(len(cur)))
		slog.Warn("chtypes refused an extra handle for a table; it keeps the handles it has",
			"handles", len(cur), "cause", describeCompileError(err))
		return nil
	}
	s.busy.Store(1)
	next := append(slices.Clip(cur), s)
	p.slots.Store(&next)
	return s
}

// release ends a call acquire started.
func (p *pool) release(s *schemaSlot) { s.busy.Add(-1) }

// close tears every handle down. See pool's lifetime note.
func (p *pool) close() {
	if p != nil {
		closeSlots(p.list())
	}
}

// describeCompileError keeps ClickHouse's own refusal (a real code) distinct
// from chtypes declining to answer — the two mean different things to an
// operator and to the HTTP status a caller picks.
func describeCompileError(err error) string {
	var se *chtypes.SchemaError
	if errors.As(err, &se) {
		return fmt.Sprintf("compile refused with ClickHouse code %d: %s", se.Code, se.Msg)
	}
	var ue *chtypes.UnsupportedError
	if errors.As(err, &ue) {
		return "chtypes declined: " + ue.Msg
	}
	return err.Error()
}

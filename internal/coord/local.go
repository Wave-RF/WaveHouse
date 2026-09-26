package coord

import (
	"context"
	"sync"
)

// Local is the in-process Coordinator: the first TryAcquire of a name wins
// and the term never expires, so a single process behaves exactly as it
// would with no coordination at all. Construct with NewLocal.
type Local struct {
	table *localTable

	mu     sync.Mutex
	closed bool
	terms  map[*localTerm]struct{}
}

// localTable is the lease state every handle over it shares.
type localTable struct {
	mu     sync.Mutex
	held   map[string]*localTerm
	tokens map[string]uint64
}

// NewLocal returns a Coordinator over a lease table of its own.
func NewLocal() *Local {
	return newLocal(&localTable{held: map[string]*localTerm{}, tokens: map[string]uint64{}})
}

func newLocal(table *localTable) *Local {
	return &Local{table: table, terms: map[*localTerm]struct{}{}}
}

// Peer returns another Coordinator over the same lease table, as a second
// process would hold one over a shared backend: it contends for the same
// names and closes independently.
func (l *Local) Peer() *Local { return newLocal(l.table) }

// TryAcquire implements Coordinator.
func (l *Local) TryAcquire(ctx context.Context, name string) (Term, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Order: handle, then table — the one order every path takes.
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, ErrClosed
	}
	l.table.mu.Lock()
	defer l.table.mu.Unlock()
	if _, ok := l.table.held[name]; ok {
		return nil, ErrHeld
	}
	l.table.tokens[name]++
	t := &localTerm{owner: l, name: name, token: l.table.tokens[name], done: make(chan struct{})}
	l.table.held[name] = t
	l.terms[t] = struct{}{}
	return t, nil
}

// Close implements Coordinator.
func (l *Local) Close(context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	for t := range l.terms {
		l.endLocked(t, nil)
	}
	return nil
}

// endLocked frees t's lease, records why, and closes its Done; l.mu is held.
func (l *Local) endLocked(t *localTerm, err error) {
	if _, ok := l.terms[t]; !ok {
		return
	}
	delete(l.terms, t)
	l.table.mu.Lock()
	delete(l.table.held, t.name)
	l.table.mu.Unlock()
	t.err = err
	close(t.done)
}

type localTerm struct {
	owner *Local
	name  string
	token uint64
	done  chan struct{}
	err   error // guarded by owner.mu
}

func (t *localTerm) Name() string          { return t.name }
func (t *localTerm) Token() uint64         { return t.token }
func (t *localTerm) Done() <-chan struct{} { return t.done }

func (t *localTerm) Err() error {
	t.owner.mu.Lock()
	defer t.owner.mu.Unlock()
	return t.err
}

func (t *localTerm) Resign(context.Context) error {
	t.owner.mu.Lock()
	defer t.owner.mu.Unlock()
	t.owner.endLocked(t, nil)
	return nil
}

package mq

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// natsPermissionWatch holds the subjects the server refused a connection's
// publish to. The server reports a refused publish only as an asynchronous
// -ERR naming the subject, which nats.go hands to the connection's error
// handler and to no request: a refused request just goes unanswered.
type natsPermissionWatch struct {
	mu sync.Mutex
	// denied is each refused subject's latest refusal, numbered by seq.
	denied map[string]uint64
	seq    uint64
	// changed is closed, and replaced, on every refusal.
	changed chan struct{}
}

func newNATSPermissionWatch() *natsPermissionWatch {
	return &natsPermissionWatch{denied: map[string]uint64{}, changed: make(chan struct{})}
}

// mark is the latest refusal's number, for deniedSince.
func (w *natsPermissionWatch) mark() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seq
}

// changes closes at the next refusal.
func (w *natsPermissionWatch) changes() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.changed
}

// publishViolation matches the server's report of a refused publish.
var publishViolation = regexp.MustCompile(`(?i)permissions violation for publish to "([^"]+)"`)

// record notes the subject of err when it reports a refused publish, and
// returns it, and whether it is the first refusal of that subject.
func (w *natsPermissionWatch) record(err error) (subject string, first, ok bool) {
	if err == nil || !errors.Is(err, nats.ErrPermissionViolation) {
		return "", false, false
	}
	m := publishViolation.FindStringSubmatch(err.Error())
	if m == nil {
		return "", false, false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	_, seen := w.denied[m[1]]
	w.seq++
	w.denied[m[1]] = w.seq
	close(w.changed)
	w.changed = make(chan struct{})
	return m[1], !seen, true
}

// deniedSince reports whether the server refused a publish to subject after
// the refusal numbered since: permissions change, so an older refusal says
// nothing about a new request.
func (w *natsPermissionWatch) deniedSince(subject string, since uint64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.denied[subject] > since
}

// list is every subject the server refused, sorted.
func (w *natsPermissionWatch) list() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Sorted(maps.Keys(w.denied))
}

// isConsumerAPI reports whether subject is a JetStream consumer API request
// under any domain's prefix: $JS.API.CONSUMER.… or $JS.<domain>.API.CONSUMER.….
func isConsumerAPI(subject string) bool {
	rest, ok := strings.CutPrefix(subject, "$JS.")
	if !ok {
		return false
	}
	if !strings.HasPrefix(rest, "API.") {
		_, rest, _ = strings.Cut(rest, ".")
	}
	return strings.HasPrefix(rest, "API.CONSUMER.")
}

// natsProbeWait bounds one permission probe. The server answers an allowed
// probe at once and reports a refused one at once, so it only has to cover a
// slow round trip.
const natsProbeWait = 2 * time.Second

// natsProbeParallel bounds the probes in flight.
const natsProbeParallel = 64

// natsPermissionProbes are the shard durable requests a probe makes, each one
// the server refuses on its merits and acts on in no other way: a pull whose
// heartbeat is more than half its expiry, and an unpin naming no priority
// group. RESET is not probed: an empty or valid one resets the durable.
var natsPermissionProbes = []struct {
	verb    string
	payload []byte
}{
	{"MSG.NEXT", []byte(`{"expires":1000000000,"heartbeat":1000000000}`)},
	{"UNPIN", []byte(`{}`)},
}

// probePermissions adds a required finding for each shard durable the
// connecting user may not pull from or unpin. A user's permissions name every
// shard's durable, so a set generated for fewer shards than the topology has
// leaves the others' rows unpulled while their publishes still succeed. It
// needs the connection's refused publishes in v.perms; a probe that got
// neither an answer nor a refusal is an error, a check that could not run.
func (v *topologyVerifier) probePermissions(ctx context.Context) error {
	if v.perms == nil || len(v.probe) == 0 {
		return nil
	}
	opts := v.js.Options()
	api := natsAPIPrefix(opts.Domain)
	if opts.Domain == "" && opts.APIPrefix != "" {
		api = strings.TrimSuffix(opts.APIPrefix, ".") + "."
	}
	nc := v.js.Conn()
	type result struct {
		unit    natsUnit
		subject string
		denied  bool
		err     error
	}
	var (
		wg      sync.WaitGroup
		results = make(chan result, len(v.probe)*len(natsPermissionProbes))
		slots   = make(chan struct{}, natsProbeParallel)
	)
	for _, u := range v.probe {
		for _, p := range natsPermissionProbes {
			subject := api + "CONSUMER." + p.verb + "." + u.stream + "." + u.durable
			wg.Go(func() {
				slots <- struct{}{}
				defer func() { <-slots }()
				denied, err := v.probeOne(ctx, nc, subject, p.payload)
				if denied || err != nil {
					results <- result{unit: u, subject: subject, denied: denied, err: err}
				}
			})
		}
	}
	wg.Wait()
	close(results)
	sorted := slices.SortedFunc(func(yield func(result) bool) {
		for r := range results {
			if !yield(r) {
				return
			}
		}
	}, func(a, b result) int { return strings.Compare(a.subject, b.subject) })
	hint := "wavehouse mq permissions --shards " + fmt.Sprint(v.t.Shards)
	if opts.Domain != "" {
		hint += " --js-domain " + opts.Domain
	}
	var errs []error
	denied := map[string]bool{}
	for _, r := range sorted {
		switch {
		case r.denied && !denied[r.unit.id()]:
			denied[r.unit.id()] = true
			v.add(FindingRequired, "consumer "+r.unit.id(), "permissions",
				"the connecting user may not publish to %s, so this shard's rows are never pulled; regenerate its permissions with `%s`", r.subject, hint)
		case r.err != nil:
			errs = append(errs, fmt.Errorf("probe %s: %w", r.subject, r.err))
		}
	}
	return errors.Join(errs...)
}

// probeOne sends one probe request, returning once it is answered (allowed)
// or the server refuses it (denied), whichever comes first. No answer and no
// refusal within natsProbeWait is an error.
func (v *topologyVerifier) probeOne(ctx context.Context, nc *nats.Conn, subject string, payload []byte) (denied bool, err error) {
	since := v.perms.mark()
	rctx, cancel := context.WithTimeout(ctx, natsProbeWait)
	defer cancel()
	answered := make(chan error, 1)
	go func() {
		_, err := nc.RequestWithContext(rctx, subject, payload)
		answered <- err
	}()
	for {
		changed := v.perms.changes()
		if v.perms.deniedSince(subject, since) {
			cancel()
			<-answered
			return true, nil
		}
		select {
		case err := <-answered:
			if err != nil && v.perms.deniedSince(subject, since) {
				return true, nil
			}
			return false, err
		case <-changed:
		}
	}
}

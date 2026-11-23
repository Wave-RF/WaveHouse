package mq

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wave-RF/WaveHouse/internal/observability"
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
	// probed is the subjects outside the JetStream API whose refusals are
	// kept too: the ones a probe publishes to.
	probed map[string]bool
}

func newNATSPermissionWatch() *natsPermissionWatch {
	return &natsPermissionWatch{denied: map[string]uint64{}, changed: make(chan struct{}), probed: map[string]bool{}}
}

// probing keeps the refusals of subject, which a probe is about to publish
// to, though it is not a JetStream API subject.
func (w *natsPermissionWatch) probing(subject string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.probed[subject] = true
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

// record notes the subject of err when it reports a refused publish to a
// JetStream API subject, or to one a probe publishes to, and returns it, and
// whether it is the first refusal of that subject. Only those are kept: they
// are bounded by the topology, where an ack's subject is new with every row,
// and a row's with every table.
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
	if plainAPISubject(m[1]) == "" && !w.probed[m[1]] {
		return "", false, false
	}
	_, seen := w.denied[m[1]]
	w.seq++
	w.denied[m[1]] = w.seq
	close(w.changed)
	w.changed = make(chan struct{})
	return m[1], !seen, true
}

// deniedSince reports whether the server refused a publish to subject after
// the refusal numbered since: permissions change, so an older refusal says
// nothing about a new request. A server in the subject's JetStream domain
// maps it to the plain $JS.API form before it checks, and reports that one.
func (w *natsPermissionWatch) deniedSince(subject string, since uint64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.denied[subject] > since || w.denied[plainAPISubject(subject)] > since
}

// list is every subject the server refused, sorted.
func (w *natsPermissionWatch) list() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Sorted(maps.Keys(w.denied))
}

// plainAPISubject is a JetStream API subject in its plain form,
// $JS.API.…, whether it came as that or as $JS.<domain>.API.…; "" for any
// other subject.
func plainAPISubject(subject string) string {
	rest, ok := strings.CutPrefix(subject, "$JS.")
	if !ok {
		return ""
	}
	if !strings.HasPrefix(rest, "API.") {
		_, rest, _ = strings.Cut(rest, ".")
		if !strings.HasPrefix(rest, "API.") {
			return ""
		}
	}
	return "$JS." + rest
}

// isConsumerAPI reports whether subject is a JetStream consumer API request
// under any domain's prefix: $JS.API.CONSUMER.… or $JS.<domain>.API.CONSUMER.….
func isConsumerAPI(subject string) bool {
	return strings.HasPrefix(plainAPISubject(subject), "$JS.API.CONSUMER.")
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

// natsReadProbe asks a stream for the message at a sequence none has, which
// both the direct get and the leader's message get answer with "not found":
// what a replay reads a partition's waiting rows by.
var natsReadProbe = []byte(`{"seq":18446744073709551615}`)

// natsProbeToken stands in a probe's subject where a tenant would be: no
// tenant id can be it.
const natsProbeToken = "$probe"

// probePermissions adds a required finding for each thing the connecting
// user may not do that WaveHouse does: pull from or unpin a shard durable,
// read a partition's waiting rows, record a row on the history, or subscribe
// to the live subject. A user's permissions name every shard's durable, so a
// set generated for fewer shards than the topology has leaves the others'
// rows unpulled while their publishes still succeed; and one generated by an
// earlier WaveHouse leaves the history empty and every stream silent, with
// ingest still answering 200. It needs the connection's refused publishes in
// v.perms; a probe that got neither an answer nor a refusal is an error, a
// check that could not run.
func (v *topologyVerifier) probePermissions(ctx context.Context) error {
	if v.perms == nil {
		return nil
	}
	opts := v.js.Options()
	api := natsAPIPrefix(opts.Domain)
	if opts.Domain == "" && opts.APIPrefix != "" {
		api = strings.TrimSuffix(opts.APIPrefix, ".") + "."
	}
	nc := v.js.Conn()
	hint := "wavehouse mq permissions " + permissionFlags(v.t, opts.Domain)

	// probe is one request to make, with what it is about and what a refusal
	// costs, for the finding: one finding an object, however many of its
	// probes are refused.
	type probe struct {
		msg           *nats.Msg
		object, costs string
		denied        bool
		err           error
	}
	var probes []*probe
	add := func(subject string, payload []byte, object, costs string) *probe {
		p := &probe{msg: &nats.Msg{Subject: subject, Data: payload}, object: object, costs: costs}
		probes = append(probes, p)
		return p
	}
	for _, u := range v.probe {
		for _, p := range natsPermissionProbes {
			add(api+"CONSUMER."+p.verb+"."+u.stream+"."+u.durable, p.payload, "consumer "+u.id(), "this shard's rows are never pulled")
		}
	}
	// A direct get no replica answers is a stream that does not allow them,
	// which is a finding of its own.
	for _, stream := range v.direct {
		add(api+"DIRECT.GET."+stream, natsReadProbe, "stream "+stream, "an SSE replay cannot read the rows waiting on this partition")
	}
	for _, stream := range v.partitions {
		add(api+"STREAM.MSG.GET."+stream, natsReadProbe, "stream "+stream, "an SSE replay cannot read the rows waiting on this partition")
	}
	if v.history {
		// A publish that expects a stream no stream is named is refused on its
		// merits, and stores nothing.
		subject := v.t.Prefix + ".hist." + natsProbeToken
		v.perms.probing(subject)
		p := add(subject, nil, "stream "+v.t.HistoryStream, "no written row is recorded for SSE replay")
		p.msg.Header = nats.Header{jetstream.ExpectedStreamHeader: []string{natsProbeToken}}
	}

	var (
		wg    sync.WaitGroup
		slots = make(chan struct{}, natsProbeParallel)
	)
	for _, p := range probes {
		wg.Go(func() {
			defer observability.Crash("mq_permission_probe")
			slots <- struct{}{}
			defer func() { <-slots }()
			p.denied, p.err = v.probeOne(ctx, nc, p.msg)
		})
	}
	wg.Wait()
	slices.SortStableFunc(probes, func(a, b *probe) int { return strings.Compare(a.msg.Subject, b.msg.Subject) })
	var errs []error
	denied := map[string]bool{}
	for _, p := range probes {
		switch {
		case p.denied && !denied[p.object]:
			denied[p.object] = true
			v.add(FindingRequired, p.object, "permissions",
				"the connecting user may not publish to %s, so %s; regenerate its permissions with `%s`", p.msg.Subject, p.costs, hint)
		case p.err != nil:
			errs = append(errs, fmt.Errorf("probe %s: %w", p.msg.Subject, p.err))
		}
	}

	live := natsLiveSubjects(v.t.Prefix)
	refused, err := probeSubscribe(nc, live)
	switch {
	case err != nil:
		errs = append(errs, fmt.Errorf("probe a subscription to %s: %w", live, err))
	case refused:
		v.add(FindingRequired, "subject "+live, "permissions",
			"the connecting user may not subscribe to %s, so no event reaches a live SSE stream; regenerate its permissions with `%s`", live, hint)
	}
	return errors.Join(errs...)
}

// probeSubscribe reports whether the server refuses the connection a
// subscription to subject. The server answers a refused one before the flush
// that follows it, and the client hands that answer to the subscription
// (nats.PermissionErrOnSubscribe, which the connection must be opened with:
// without it no refusal is seen).
func probeSubscribe(nc *nats.Conn, subject string) (refused bool, err error) {
	sub, err := nc.SubscribeSync(subject)
	if err != nil {
		return false, err
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.FlushTimeout(natsProbeWait); err != nil {
		return false, err
	}
	// A subscription the server holds just has nothing to hand over yet.
	_, err = sub.NextMsg(time.Millisecond)
	return errors.Is(err, nats.ErrPermissionViolation), nil
}

// probeOne sends one probe request, returning once it is answered (allowed)
// or the server refuses it (denied), whichever comes first. No answer and no
// refusal within natsProbeWait is an error.
func (v *topologyVerifier) probeOne(ctx context.Context, nc *nats.Conn, msg *nats.Msg) (denied bool, err error) {
	subject := msg.Subject
	since := v.perms.mark()
	rctx, cancel := context.WithTimeout(ctx, natsProbeWait)
	defer cancel()
	answered := make(chan error, 1)
	go func() {
		defer observability.Crash("mq_permission_probe")
		_, err := nc.RequestMsgWithContext(rctx, msg)
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

// permissionFlags is the `wavehouse mq permissions` command line for t under
// domain: the shard count, then each other setting that is not its default.
func permissionFlags(t NATSTopology, domain string) string {
	flags := "--shards " + strconv.Itoa(t.Shards)
	if t.Prefix != DefaultNATSSubjectPrefix {
		flags += " --prefix " + t.Prefix
	}
	if t.IngestConsumer != DefaultNATSIngestConsumer {
		flags += " --ingest-consumer " + t.IngestConsumer
	}
	if t.HistoryStream != t.streamName("HISTORY") {
		flags += " --history-stream " + t.HistoryStream
	}
	if t.CoordBucket != "" && t.CoordBucket != DefaultNATSCoordBucket(t.Prefix) {
		flags += " --coord-bucket " + t.CoordBucket
	}
	if domain != "" {
		flags += " --js-domain " + domain
	}
	return flags
}

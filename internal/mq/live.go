package mq

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/Wave-RF/WaveHouse/internal/observability"
)

// The live feed, the same on both brokers. An ingest stream is a work queue,
// which refuses a second consumer on its subjects, so the events are not read
// from it: the stream republishes every row it stores to a plain subject no
// stream holds, and each process that relays events subscribes to that
// subject. A row reaches the feed once, when the queue has stored it, in the
// order the queue stored it, with the headers its publisher set.

// The headers the server sets on a republished row: the stream that stored
// it, and its sequence there.
const (
	republishedStream   = "Nats-Stream"
	republishedSequence = "Nats-Sequence"
)

// liveFeed is one subscription to a republish subject. A plain subscription
// has no redelivery, so the feed watches for rows it was not sent: every
// stream's rows carry its sequence, which rises by one a row, and a
// connection that was lost may have missed any number. Either way lost is
// called before a later row is delivered.
type liveFeed struct {
	// deliver hands one republished row to the subscriber.
	deliver func(m *nats.Msg)
	lost    func()
	// connected reports whether the broker's connection is up: a row
	// delivered while it is down was received before it went.
	connected func() bool

	// mu makes a call to lost finish before the next row is delivered.
	mu sync.Mutex
	// last is the sequence of each stream's last row delivered.
	last map[string]uint64
	// gap is set while the connection has been lost and lost has not been
	// called for it.
	gap bool
}

func newLiveFeed(deliver func(m *nats.Msg), lost func(), connected func() bool) *liveFeed {
	return &liveFeed{deliver: deliver, lost: lost, connected: connected, last: map[string]uint64{}}
}

// receive is the subscription's handler: it delivers m, after reporting any
// row skipped ahead of it. A message that is not a republished row was not
// stored by the queue, and is dropped.
func (f *liveFeed) receive(m *nats.Msg) {
	stream := m.Header.Get(republishedStream)
	seq, err := strconv.ParseUint(m.Header.Get(republishedSequence), 10, 64)
	if stream == "" || err != nil {
		slog.Debug("mq: dropped a message on the live subject that no ingest stream republished", "component", "nats", "subject", m.Subject)
		return
	}
	f.mu.Lock()
	last, seen := f.last[stream]
	if (f.gap && f.connected()) || (seen && seq != last+1) {
		f.report()
	}
	f.last[stream] = seq
	f.mu.Unlock()
	f.deliver(m)
}

// report calls lost for everything skipped so far. Every reader there was is
// sent back to the replay, so no stream's earlier sequence means anything to
// the ones that come after. Under mu.
func (f *liveFeed) report() {
	f.gap = false
	clear(f.last)
	f.lost()
}

// disconnected notes that the connection is gone: whatever the queues store
// until it is back never reaches this subscription.
func (f *liveFeed) disconnected() {
	f.mu.Lock()
	f.gap = true
	f.mu.Unlock()
}

// reconnected reports the rows a lost connection skipped as soon as it is
// back, rather than at the next row, which a quiet table's readers could wait
// on for long. Not before: a reader sent back to the replay while the broker
// cannot be reached would find none.
func (f *liveFeed) reconnected() {
	f.mu.Lock()
	if f.gap {
		f.report()
	}
	f.mu.Unlock()
}

// subscribeTimeout bounds the round trip that puts a live subscription in
// place on the server.
const subscribeTimeout = 5 * time.Second

// subscribeLive subscribes feed to subject on nc until ctx is done or stop is
// called, either of which closes stopped. The rows arrive one at a time, on
// the client's delivery goroutine.
func subscribeLive(ctx context.Context, nc *nats.Conn, subject string, feed *liveFeed) (stop func(), stopped <-chan struct{}, err error) {
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		defer observability.Crash("mq_delivery")
		feed.receive(m)
	})
	if err != nil {
		return nil, nil, err
	}
	// Answered once the server holds the subscription, so a row stored after
	// this returns is delivered.
	if err := nc.FlushTimeout(subscribeTimeout); err != nil {
		_ = sub.Unsubscribe()
		return nil, nil, err
	}
	done := make(chan struct{})
	var once sync.Once
	stop = func() {
		once.Do(func() {
			_ = sub.Unsubscribe()
			close(done)
		})
	}
	go func() {
		defer observability.Crash("mq_live")
		select {
		case <-ctx.Done():
		case <-done:
		}
		stop()
	}()
	return stop, done, nil
}

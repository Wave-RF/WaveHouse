package mq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The replay history, the same on both brokers: a limits-retention stream the
// ingest worker writes each row to once the row is in ClickHouse, before it
// acks the row off the ingest queue (Recorder). The stream's own age limit
// then counts from the write, so a row stays replayable for the window after
// it was written, however long it waited to be.

const (
	// acceptedHeader carries, on a history row, when the ingest queue accepted
	// it: the history's own timestamp is when it was written, and a replay
	// asks for what was accepted since a time.
	acceptedHeader = "Wh-Accepted"
	// recordWindow is how many rows of a batch are in flight to the history at
	// once.
	recordWindow = 256
)

// historyWriter records written rows on a history stream.
type historyWriter struct {
	// js publishes the rows; its asynchronous publishes give up after timeout.
	js jetstream.JetStream
	// timeout bounds one call to record: the acks of the batch wait on it.
	timeout time.Duration
}

// newHistoryWriter is a writer over a JetStream context of its own on nc,
// under domain when set, so its publish timeout is no other publisher's.
func newHistoryWriter(nc *nats.Conn, domain string, timeout time.Duration) (historyWriter, error) {
	opts := []jetstream.JetStreamOpt{jetstream.WithPublishAsyncTimeout(timeout)}
	var (
		js  jetstream.JetStream
		err error
	)
	if domain != "" {
		js, err = jetstream.NewWithDomain(nc, domain, opts...)
	} else {
		js, err = jetstream.New(nc, opts...)
	}
	if err != nil {
		return historyWriter{}, fmt.Errorf("jetstream: %w", err)
	}
	return historyWriter{js: js, timeout: timeout}, nil
}

// record publishes each of msgs to the subject subjectOf gives it, in order,
// a window at a time, with the time the queue accepted it; a message
// subjectOf has no subject for is skipped (its tenant keeps no history). It
// returns once every row is stored, refused, or timed out, which is within
// the writer's timeout of the call; the error counts the rows not stored.
// The whole call is one mq.publish span of kind history, and none when there
// is no row to record.
func (h historyWriter) record(ctx context.Context, msgs []*Message, subjectOf func(*Message) (string, bool), opts ...jetstream.PublishOpt) (err error) {
	rows := make([]*nats.Msg, 0, len(msgs))
	for _, m := range msgs {
		subj, ok := subjectOf(m)
		if !ok {
			continue
		}
		out := nats.NewMsg(subj)
		out.Data = m.Data
		if !m.accepted.IsZero() {
			out.Header.Set(acceptedHeader, m.accepted.UTC().Format(time.RFC3339Nano))
		}
		rows = append(rows, out)
	}
	if len(rows) == 0 {
		return nil
	}
	total := len(rows)
	spanCtx, end := beginPublish(ctx, publishKindHistory)
	defer func() { end(1, err) }()
	spanCtx, cancel := context.WithTimeout(spanCtx, h.timeout)
	defer cancel()
	deadline, _ := spanCtx.Deadline()

	var (
		failed int
		first  error
	)
	lose := func(err error) {
		failed++
		if first == nil {
			first = err
		}
	}
	for len(rows) > 0 {
		window := rows[:min(len(rows), recordWindow)]
		rows = rows[len(window):]
		futures := make([]jetstream.PubAckFuture, 0, len(window))
		for _, out := range window {
			wait := time.Until(deadline)
			if wait <= 0 {
				lose(context.DeadlineExceeded)
				continue
			}
			// A window another batch still has in flight makes this publish wait
			// for room rather than fail at once.
			f, err := h.js.PublishMsgAsync(out, append([]jetstream.PublishOpt{jetstream.WithStallWait(wait)}, opts...)...)
			if err != nil {
				lose(err)
				continue
			}
			futures = append(futures, f)
		}
		for _, f := range futures {
			select {
			case <-f.Ok():
			case err := <-f.Err():
				lose(err)
			case <-spanCtx.Done():
				lose(spanCtx.Err())
			}
		}
	}
	switch {
	case failed == 0:
		return nil
	case noAnswer(first) || errors.Is(first, jetstream.ErrAsyncPublishTimeout):
		return fmt.Errorf("%w: %d of %d rows not recorded: %w", ErrUnavailable, failed, total, first)
	}
	return fmt.Errorf("%d of %d rows not recorded: %w", failed, total, first)
}

// acceptedAt is when the ingest queue accepted the event a history row holds:
// what the row says, or for a row that does not say — one a partition
// republished at acceptance, before the worker wrote the history — when the
// history stored it.
func acceptedAt(m jetstream.Msg) time.Time {
	if v := m.Headers().Get(acceptedHeader); v != "" {
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return t
		}
	}
	if meta, err := m.Metadata(); err == nil {
		return meta.Timestamp
	}
	return time.Time{}
}

package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The replay, the same on both brokers. An event the broker holds is in one of
// two places: on its ingest stream, a work queue, until the worker has written
// it, and from then on in the history stream, for as long as that keeps it.
// The worker records it in the history before it acks it off the queue, so it
// is never in neither. A replay reads the history, then the queue, then what
// reached the history while it read the queue: an event that moved meanwhile
// is sent twice, and none is missed.

const (
	// replayPullWait bounds one read of a replay — a pull of history rows the
	// server has already counted, or one answer to a read of the queue — and
	// replayBatch is how many rows one read asks for: a replay is a round trip
	// per batch, not per event.
	replayPullWait = 2 * time.Second
	replayBatch    = 256
	// replayInactiveThreshold is how long the server keeps the history
	// consumer of a replay whose process went away. It also has to outlast
	// sending one fetched batch to a slow SSE client, since no pull is waiting
	// meanwhile; a finished replay deletes its own.
	replayInactiveThreshold = time.Minute
	// jsErrNoMessage is the server's answer to a message get nothing matches.
	jsErrNoMessage jetstream.ErrorCode = 10037
)

// replaySource is where one topic's events are replayed from.
type replaySource struct {
	nc *nats.Conn
	js jetstream.JetStream
	// apiPrefix is the JetStream API's subject prefix, the domain's if set.
	apiPrefix string
	// history is the stream the topic's written events are in, under
	// historySubject; queues the ingest streams its waiting ones can be on.
	history, historySubject string
	queues                  []replayQueue
	// consumer is the template of the consumers the history is read through:
	// ack-less and expiring once idle.
	consumer jetstream.ConsumerConfig
}

// replayQueue is an ingest stream and the subject, or the wildcard, a topic's
// waiting events are under in it.
type replayQueue struct{ stream, subject string }

// replay sends the topic's events accepted at or after since: the written
// ones the history holds, in the order they were written, then the ones
// waiting on the queue, in the order they were accepted, then the ones written
// meanwhile. It ends when send returns false or everything stored when it
// began has been sent.
func (r replaySource) replay(ctx context.Context, since time.Time, send func(data []byte) bool) error {
	// What the queues store from here on is not sent: the live feed carries it.
	lasts := make([]uint64, len(r.queues))
	for i, q := range r.queues {
		queue, err := r.js.Stream(ctx, q.stream)
		if err != nil {
			return fmt.Errorf("replay: ingest stream %s: %w", q.stream, err)
		}
		lasts[i] = queue.CachedInfo().State.LastSeq
	}
	hist, err := r.js.Stream(ctx, r.history)
	if err != nil {
		return fmt.Errorf("replay: history stream %s: %w", r.history, err)
	}
	// Everything the history stores from here on is read after the queues.
	read := hist.CachedInfo().State.LastSeq

	start := r.consumer
	if since.IsZero() {
		start.DeliverPolicy = jetstream.DeliverAllPolicy
	} else {
		start.DeliverPolicy, start.OptStartTime = jetstream.DeliverByStartTimePolicy, &since
	}
	if more, err := r.written(ctx, start, since, send); err != nil || !more {
		return err
	}
	for i, q := range r.queues {
		if more, err := r.waiting(ctx, q, lasts[i], since, send); err != nil || !more {
			return err
		}
	}

	info, err := hist.Info(ctx)
	if err != nil {
		return fmt.Errorf("replay: history stream %s: %w", r.history, err)
	}
	if info.State.LastSeq == read {
		return nil // nothing was written while the queue was read
	}
	rest := r.consumer
	rest.DeliverPolicy, rest.OptStartSeq = jetstream.DeliverByStartSequencePolicy, read+1
	_, err = r.written(ctx, rest, since, send)
	return err
}

// written sends the topic's history rows cfg starts at that were accepted at
// or after since, through a consumer of its own, deleted when done. It reads
// the rows the server counted when the consumer was created, not the ones
// stored after. more is false once send has returned false.
func (r replaySource) written(ctx context.Context, cfg jetstream.ConsumerConfig, since time.Time, send func(data []byte) bool) (more bool, err error) {
	cfg.FilterSubject = r.historySubject
	cons, err := r.js.CreateConsumer(ctx, r.history, cfg)
	if err != nil {
		return false, fmt.Errorf("replay consumer: %w", err)
	}
	defer r.dropConsumer(cons.CachedInfo().Name)

	remaining := cons.CachedInfo().NumPending
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		batch, err := cons.Fetch(int(min(remaining, replayBatch)), jetstream.FetchMaxWait(replayPullWait)) //nolint:gosec // capped
		if err != nil {
			return false, fmt.Errorf("replay fetch: %w", err)
		}
		got := 0
		for msg := range batch.Messages() {
			got++
			remaining--
			if !since.IsZero() && acceptedAt(msg).Before(since) {
				continue // written since, but accepted before
			}
			if !send(msg.Data()) {
				return false, nil
			}
			if err := r.usable(ctx); err != nil {
				return false, err
			}
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) {
			return false, fmt.Errorf("replay fetch: %w", err)
		}
		if got == 0 {
			// The history dropped what was left (its age limit) while
			// connected: that is caught up. A pull that raced the connection
			// closing can end the same way, and that is not.
			if r.nc.IsConnected() {
				return true, nil
			}
			return false, fmt.Errorf("replay fetch: %w", nats.ErrConnectionClosed)
		}
	}
	return true, nil
}

// usable reports why a replay under way cannot go on: its context is done, or
// the connection is closing, which a read can otherwise pass for caught up.
func (r replaySource) usable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.nc.IsClosed() || r.nc.IsDraining() {
		return fmt.Errorf("replay: %w", nats.ErrConnectionClosed)
	}
	return nil
}

// dropConsumer deletes a finished replay's consumer rather than leaving it to
// expire, best effort.
func (r replaySource) dropConsumer(name string) {
	if !r.nc.IsConnected() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = r.js.DeleteConsumer(ctx, r.history, name)
}

// msgGetRequest is a read of a stream's messages with no consumer: the next
// one on a subject at or after a sequence or a time, or with Batch, on a
// stream that allows direct gets, up to that many of them.
type msgGetRequest struct {
	Seq       uint64     `json:"seq,omitempty"`
	NextFor   string     `json:"next_by_subj,omitempty"`
	Batch     int        `json:"batch,omitempty"`
	StartTime *time.Time `json:"start_time,omitempty"`
}

// nextFrom is a read of subject starting where since says, or at the first
// message for none.
func nextFrom(subject string, since time.Time) msgGetRequest {
	if since.IsZero() {
		return msgGetRequest{Seq: 1, NextFor: subject}
	}
	return msgGetRequest{StartTime: &since, NextFor: subject}
}

// waiting sends the topic's events still on ingest stream q that the stream
// stored at or after since, up to sequence last, its newest when the replay
// began. A work queue takes no second consumer, so they are read by direct
// get, a batch a request, which any replica may answer; one that trails the
// leader has not got the newest rows yet, and they were accepted before the
// replay began, so no live event will carry them. So the leader is asked last
// for whatever follows the last row read. more is false once send has returned
// false.
func (r replaySource) waiting(ctx context.Context, q replayQueue, last uint64, since time.Time, send func(data []byte) bool) (more bool, err error) {
	if last == 0 {
		return true, nil
	}
	inbox := r.nc.NewInbox()
	replies, err := r.nc.SubscribeSync(inbox)
	if err != nil {
		return false, fmt.Errorf("replay: %w", err)
	}
	defer func() { _ = replies.Unsubscribe() }()

	req := nextFrom(q.subject, since)
	var cursor uint64 // the last sequence read
	for {
		req.Batch = replayBatch
		read, pending, done, err := r.directGet(ctx, q.stream, replies, inbox, req, last, send)
		if err != nil {
			return false, err
		}
		if done {
			return false, nil
		}
		if read > 0 {
			cursor = read
		}
		// A replica that answered with a row past last has every row up to it.
		if pending == 0 || read == 0 || read > last {
			break
		}
		req = msgGetRequest{Seq: read + 1, NextFor: q.subject}
	}

	req = nextFrom(q.subject, since)
	if cursor > 0 {
		req = msgGetRequest{Seq: cursor + 1, NextFor: q.subject}
	}
	for {
		seq, data, found, err := r.leaderGet(ctx, q.stream, req)
		if err != nil {
			return false, err
		}
		if !found || seq > last {
			return true, nil
		}
		if !send(data) {
			return false, nil
		}
		if err := r.usable(ctx); err != nil {
			return false, err
		}
		req = msgGetRequest{Seq: seq + 1, NextFor: q.subject}
	}
}

// directGet sends one batched direct get of the ingest stream and hands the
// rows it answers with to send, stopping at the first one past sequence last.
// read is the last sequence it took from the answer (0 for none), pending the
// rows of the subject the batch left unread, and done whether send returned
// false.
func (r replaySource) directGet(ctx context.Context, stream string, replies *nats.Subscription, inbox string, req msgGetRequest, last uint64, send func(data []byte) bool) (read, pending uint64, done bool, err error) {
	body, err := json.Marshal(req)
	if err != nil {
		return 0, 0, false, err
	}
	if err := r.nc.PublishRequest(r.apiPrefix+"DIRECT.GET."+stream, inbox, body); err != nil {
		return 0, 0, false, fmt.Errorf("replay: read the ingest stream: %w", err)
	}
	for {
		ctxWait, cancel := context.WithTimeout(ctx, replayPullWait)
		m, err := replies.NextMsgWithContext(ctxWait)
		cancel()
		if err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return 0, 0, false, cerr
			}
			return 0, 0, false, fmt.Errorf("replay: read the ingest stream %s: %w", stream, err)
		}
		switch status := m.Header.Get("Status"); status {
		case "":
			seq, err := strconv.ParseUint(m.Header.Get(republishedSequence), 10, 64)
			if err != nil {
				return 0, 0, false, fmt.Errorf("replay: read the ingest stream %s: a row with no sequence", stream)
			}
			if seq > last {
				return seq, 0, false, nil
			}
			read = seq
			if !send(m.Data) {
				return read, 0, true, nil
			}
			if err := r.usable(ctx); err != nil {
				return 0, 0, false, err
			}
		case "204": // the end of the batch
			pending, _ = strconv.ParseUint(m.Header.Get("Nats-Num-Pending"), 10, 64)
			return read, pending, false, nil
		case "404": // nothing on the subject from there on
			return read, 0, false, nil
		default:
			// 503 is no stream answering direct gets: gone, or allow_direct off.
			return 0, 0, false, fmt.Errorf("replay: read the ingest stream %s: status %s %s", stream, status, m.Header.Get("Description"))
		}
	}
}

// leaderGet asks the ingest stream's leader for the one message req names:
// its sequence and data, or found false when there is none.
func (r replaySource) leaderGet(ctx context.Context, stream string, req msgGetRequest) (seq uint64, data []byte, found bool, err error) {
	body, err := json.Marshal(req)
	if err != nil {
		return 0, nil, false, err
	}
	ctxWait, cancel := context.WithTimeout(ctx, replayPullWait)
	defer cancel()
	m, err := r.nc.RequestWithContext(ctxWait, r.apiPrefix+"STREAM.MSG.GET."+stream, body)
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return 0, nil, false, cerr
		}
		return 0, nil, false, fmt.Errorf("replay: read the ingest stream %s: %w", stream, err)
	}
	var resp struct {
		Message *struct {
			Seq  uint64 `json:"seq"`
			Data []byte `json:"data"`
		} `json:"message"`
		Error *jetstream.APIError `json:"error"`
	}
	if err := json.Unmarshal(m.Data, &resp); err != nil {
		return 0, nil, false, fmt.Errorf("replay: read the ingest stream %s: %w", stream, err)
	}
	switch {
	case resp.Error != nil && resp.Error.ErrorCode == jsErrNoMessage:
		return 0, nil, false, nil
	case resp.Error != nil:
		return 0, nil, false, fmt.Errorf("replay: read the ingest stream %s: %w", stream, resp.Error)
	case resp.Message == nil:
		return 0, nil, false, fmt.Errorf("replay: read the ingest stream %s: an answer with no message", stream)
	}
	return resp.Message.Seq, resp.Message.Data, true, nil
}

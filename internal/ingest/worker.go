package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/observability"
	"github.com/Wave-RF/WaveHouse/internal/query"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

var (
	workerMeter        = otel.Meter("wavehouse-worker")
	eventsProcessed, _ = workerMeter.Int64Counter("wavehouse_worker_events_processed")
	dlqDropped, _      = workerMeter.Int64Counter("wavehouse_worker_dlq_dropped")
)

// IngestWorker replaces the Bento stream and manages manual batching.
type IngestWorker struct {
	nc         *nats.Conn
	js         jetstream.JetStream
	httpClient *http.Client
	cache      cache.Cache
	logger     *slog.Logger

	scheme   string
	host     string
	port     string
	user     string
	password string
	db       string

	wg sync.WaitGroup // Used for graceful shutdown
}

// StartIngestWorker starts the custom background worker. Returns a stop function.
func StartIngestWorker(
	ctx context.Context, nc *nats.Conn, cache cache.Cache,
	chHost, chHTTPPort, chHTTPScheme, chUser, chPassword, chDB string,
	onFatal func(error),
) (func(context.Context) error, error) {
	host, _, err := net.SplitHostPort(chHost)
	if err != nil {
		host = chHost
	}
	if chHTTPScheme == "" {
		chHTTPScheme = "http"
	}

	logger := slog.Default().With("component", "ingest_worker")

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("initialize JetStream: %w", err)
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, mq.StreamName(), jetstream.ConsumerConfig{
		Durable:       BufferConsumerName,
		FilterSubject: "ingest.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}

	iter, err := cons.Messages()
	if err != nil {
		return nil, fmt.Errorf("create iterator: %w", err)
	}

	worker := &IngestWorker{
		nc:         nc,
		js:         js,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		cache:      cache,
		logger:     logger,
		scheme:     chHTTPScheme,
		host:       host,
		port:       chHTTPPort,
		user:       chUser,
		password:   chPassword,
		db:         chDB,
	}

	workerCtx, workerCancel := context.WithCancel(context.Background())
	worker.wg.Add(1)
	go worker.runLoop(workerCtx, iter, onFatal)

	stopFunc := func(shutdownCtx context.Context) error {
		workerCancel()

		// Wait for the worker to finish draining its current batch
		done := make(chan struct{})
		go func() {
			worker.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			return nil
		case <-shutdownCtx.Done():
			return shutdownCtx.Err()
		}
	}

	return stopFunc, nil
}

type parsedMsg struct {
	natsMsg jetstream.Msg
	msgCtx  context.Context
	event   EventMessage
	rawJSON []byte
}

func (w *IngestWorker) runLoop(ctx context.Context, iter jetstream.MessagesContext, onFatal func(error)) {
	defer w.wg.Done()
	defer iter.Stop()

	const maxBatch = 500
	const maxWait = 5 * time.Second

	msgChan := make(chan jetstream.Msg)
	go func() {
		for {
			m, err := iter.Next()
			if err != nil {
				return // Iterator closed
			}
			msgChan <- m
		}
	}()

	var batch []jetstream.Msg
	timer := time.NewTimer(maxWait)
	if !timer.Stop() {
		<-timer.C
	}

	w.logger.InfoContext(ctx, "ingest worker started")

	for {
		select {
		case <-ctx.Done():
			// Final flush before shutdown
			w.flush(context.Background(), batch)
			return
		case <-timer.C:
			// 5 seconds have passed since the FIRST message in this batch arrived
			w.flush(ctx, batch)
			batch = nil
		case m := <-msgChan:
			// If this is the first message in a new batch, start the 5-second countdown
			if len(batch) == 0 {
				timer.Reset(maxWait)
			}

			batch = append(batch, m)

			// If we hit the 500 size limit, flush immediately and stop the timer
			if len(batch) >= maxBatch {
				if !timer.Stop() {
					// drain the channel if it already fired
					select {
					case <-timer.C:
					default:
					}
				}
				w.flush(ctx, batch)
				batch = nil
			}
		}
	}
}

func (w *IngestWorker) flush(ctx context.Context, batch []jetstream.Msg) {
	if len(batch) == 0 {
		return
	}

	groups := make(map[string][]parsedMsg)

	// 1. Parse and group messages
	for _, m := range batch {
		msgCtx := observability.ExtractNATS(ctx, m)

		var raw struct {
			TableName         string          `json:"table_name"`
			Scope             string          `json:"scope,omitempty"`
			Data              json.RawMessage `json:"data"`
			ReceivedTimestamp string          `json:"received_timestamp"`
		}

		if err := json.Unmarshal(m.Data(), &raw); err != nil || raw.TableName == "" || len(raw.Data) == 0 {
			w.logger.ErrorContext(msgCtx, "rejecting invalid/empty message", "error", err)
			_ = m.DoubleAck(msgCtx) // Drop it
			continue
		}

		evt := EventMessage{
			TableName:         raw.TableName,
			Scope:             raw.Scope,
			ReceivedTimestamp: raw.ReceivedTimestamp,
		}

		groups[raw.TableName] = append(groups[raw.TableName], parsedMsg{
			natsMsg: m,
			msgCtx:  msgCtx,
			event:   evt,
			rawJSON: raw.Data, // Pass only the inner payload to ClickHouse
		})
	}

	// 2. Insert into ClickHouse per table group
	for tableName, msgs := range groups {
		err := w.insertToClickHouse(ctx, tableName, msgs)

		if err != nil {
			w.logger.ErrorContext(ctx, "clickhouse insert failed, sending to DLQ", "table", tableName, "error", err)
			w.sendToDLQ(ctx, tableName, msgs)
		} else {
			w.handleSuccess(ctx, tableName, msgs)
		}
	}
}

func (w *IngestWorker) insertToClickHouse(ctx context.Context, tableName string, msgs []parsedMsg) error {
	var buf bytes.Buffer
	for _, m := range msgs {
		buf.Write(m.rawJSON)
		buf.WriteString("\n")
	}

	q := url.Values{}
	q.Set("database", w.db)
	q.Set("param_target_table", tableName)
	q.Set("query", "INSERT INTO {target_table:Identifier} FORMAT JSONEachRow")
	q.Set("input_format_skip_unknown_fields", "1")
	q.Set("date_time_input_format", "best_effort")

	u := &url.URL{
		Scheme:   w.scheme,
		Host:     net.JoinHostPort(w.host, w.port),
		RawQuery: q.Encode(),
	}

	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ClickHouse-User", w.user)
	req.Header.Set("X-ClickHouse-Key", w.password)

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("clickhouse error %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (w *IngestWorker) handleSuccess(ctx context.Context, tableName string, msgs []parsedMsg) {
	w.logger.InfoContext(ctx, "batch successfully inserted", "table", tableName, "count", len(msgs))

	// CONCURRENT DoubleAck: Execute all network calls in parallel to eliminate latency penalty
	var ackWg sync.WaitGroup
	for _, pm := range msgs {
		ackWg.Add(1)

		go func(m jetstream.Msg, mCtx context.Context) {
			defer ackWg.Done()
			if err := m.DoubleAck(mCtx); err != nil {
				w.logger.WarnContext(mCtx, "double ack failed for processed message", "error", err)
			}
		}(pm.natsMsg, pm.msgCtx)

		eventsProcessed.Add(pm.msgCtx, 1, metric.WithAttributes(attribute.String("table", tableName)))
	}
	ackWg.Wait() // Ensure all ACKs finish before we return to the flush loop

	// Handle Cache Invalidation
	if w.cache != nil {
		scopes := make(map[string]struct{})
		for _, pm := range msgs {
			if pm.event.Scope != "" {
				scopes[pm.event.Scope] = struct{}{}
			}
		}
		invCtx := trace.ContextWithSpanContext(context.Background(), trace.SpanContextFromContext(ctx))
		if _, err := w.cache.InvalidateCache(invCtx, tableName, scopes); err != nil {
			w.logger.ErrorContext(ctx, "failed to invalidate cache after insert", "table", tableName, "error", err)
		}
	}
}

func (w *IngestWorker) sendToDLQ(ctx context.Context, tableName string, msgs []parsedMsg) {
	for _, pm := range msgs {
		subject := "dlq." + query.SafeEncodeNATS(tableName)
		if pm.event.Scope != "" {
			subject += "." + query.SafeEncodeNATS(pm.event.Scope)
		}

		if _, pubErr := w.js.Publish(ctx, subject, pm.natsMsg.Data()); pubErr != nil {
			w.logger.ErrorContext(pm.msgCtx, "NATS DLQ publish failed", "subject", subject, "error", pubErr)
			dlqDropped.Add(ctx, 1, metric.WithAttributes(attribute.String("table", tableName)))
		} else {
			w.logger.WarnContext(pm.msgCtx, "sent failed message to DLQ", "subject", subject)
		}

		// Still DoubleAck the original message so it doesn't get redelivered
		// (since we safely moved it to the DLQ)
		_ = pm.natsMsg.DoubleAck(pm.msgCtx)
	}
}

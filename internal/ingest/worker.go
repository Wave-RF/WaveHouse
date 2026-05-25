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
	"strings"
	"sync"
	"time"

	"github.com/Wave-RF/WaveHouse/internal/cache"
	"github.com/Wave-RF/WaveHouse/internal/mq"
	"github.com/Wave-RF/WaveHouse/internal/query"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/trace"
)

type parsedMsg struct {
	natsMsg         jetstream.Msg
	natsSafeSubject string
	scope           string
	rawJSON         []byte
}

type IngestWorker struct {
	js         jetstream.JetStream
	httpClient *http.Client
	cache      cache.Cache
	logger     *slog.Logger
	chURL      string
	user       string
	password   string
	db         string
	wg         sync.WaitGroup
}

func StartIngestWorker(
	ctx context.Context, nc *nats.Conn, cache cache.Cache,
	chHost, chHTTPPort, chHTTPScheme, chUser, chPassword, chDB string,
) (func(context.Context) error, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("initialize JetStream: %w", err)
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, mq.StreamName(), jetstream.ConsumerConfig{
		Durable:       BufferConsumerName,
		FilterSubject: "ingest.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: 10_000, // TODO: testing increasing this to see if NATS is bottlenecking us
	})
	if err != nil {
		return nil, err
	}

	host, _, err := net.SplitHostPort(chHost)
	if err != nil {
		host = chHost // If it fails to split, assume it's just a raw host/IP
	}

	// Tune the HTTP Transport for high-throughput ClickHouse ingestion
	customTransport := http.DefaultTransport.(*http.Transport).Clone()
	customTransport.MaxIdleConns = 100
	customTransport.MaxIdleConnsPerHost = 100
	customTransport.MaxConnsPerHost = 100
	customTransport.IdleConnTimeout = 90 * time.Second

	worker := &IngestWorker{
		js: js,
		httpClient: &http.Client{
			Transport: customTransport,
			Timeout:   30 * time.Second,
		},
		cache:    cache,
		logger:   slog.Default().With("component", "ingest_worker"),
		chURL:    fmt.Sprintf("%s://%s:%s", chHTTPScheme, host, chHTTPPort),
		user:     chUser,
		password: chPassword,
		db:       chDB,
	}

	workerCtx, workerCancel := context.WithCancel(ctx)
	worker.wg.Add(1)

	// Start push-based loop
	go worker.runLoop(workerCtx, cons)

	stopFunc := func(shutdownCtx context.Context) error {
		workerCancel()
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

func (w *IngestWorker) runLoop(ctx context.Context, cons jetstream.Consumer) {
	defer w.wg.Done()

	const maxBatch = 500
	const maxWait = 5 * time.Second

	msgChan := make(chan jetstream.Msg, maxBatch*2)

	// Push-based consumer (much faster/more efficient than iterators)
	consumeCtx, err := cons.Consume(func(msg jetstream.Msg) {
		msgChan <- msg
	})
	if err != nil {
		w.logger.Error("failed to start consumer", "error", err)
		return
	}
	defer consumeCtx.Stop()

	var batch []jetstream.Msg
	timer := time.NewTimer(maxWait)
	if !timer.Stop() {
		<-timer.C
	}

	for {
		select {
		case <-ctx.Done():
			w.flush(context.WithoutCancel(ctx), batch)
			return
		case <-timer.C:
			w.flush(ctx, batch)
			batch = nil
		case m := <-msgChan:
			if len(batch) == 0 {
				timer.Reset(maxWait)
			}
			batch = append(batch, m)
			if len(batch) >= maxBatch {
				if !timer.Stop() {
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

	for _, m := range batch {
		natsSafeSubject := strings.TrimPrefix(m.Subject(), "ingest.")

		var envelope struct {
			UnsafeTableName   string          `json:"table_name"`
			UnsafeScope       string          `json:"scope"`
			ReceivedTimestamp string          `json:"received_timestamp"`
			Data              json.RawMessage `json:"data"`
		}

		if err := json.Unmarshal(m.Data(), &envelope); err != nil {
			w.logger.ErrorContext(ctx, "failed to parse event envelope", "error", err)
			_ = m.DoubleAck(ctx)
			continue
		}

		groups[envelope.UnsafeTableName] = append(groups[envelope.UnsafeTableName], parsedMsg{
			natsMsg:         m,
			natsSafeSubject: natsSafeSubject,
			scope:           envelope.UnsafeScope,
			rawJSON:         envelope.Data, // Pass only the inner payload to ClickHouse
		})
	}

	var tableWg sync.WaitGroup

	for tableName, msgs := range groups {
		tableWg.Add(1)

		go func(tableName string, msgs []parsedMsg) {
			defer tableWg.Done()

			// Attempt bulk insert
			err := w.insertToClickHouse(ctx, tableName, msgs)

			if err == nil {
				// Success – Ack the batch
				w.handleSuccess(ctx, tableName, msgs)
				return
			}

			w.logger.WarnContext(ctx, "bulk insert failed, falling back to 1-by-1 isolation", "table", tableName, "error", err)

			// ISOLATE & DLQ: Attempt 1-by-1 insertion to isolate the bad row
			// TODO: potentially could try a binary search or something eventually maybe? unclear if faster...
			for _, pm := range msgs {
				singleErr := w.insertToClickHouse(ctx, tableName, []parsedMsg{pm})
				if singleErr != nil {
					w.logger.ErrorContext(ctx, "isolated bad row, sending to DLQ", "table", tableName, "error", singleErr)
					w.sendToDLQ(ctx, tableName, pm, singleErr.Error())
				} else {
					w.handleSuccess(ctx, tableName, []parsedMsg{pm})
				}
			}
		}(tableName, msgs)
	}

	tableWg.Wait()
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

	req, err := http.NewRequestWithContext(ctx, "POST", w.chURL+"?"+q.Encode(), &buf)
	if err != nil {
		return fmt.Errorf("failed to create http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ClickHouse-User", w.user)
	req.Header.Set("X-ClickHouse-Key", w.password)

	// TODO: future optimization: could build list for cache invalidation here while waiting on the network request
	resp, err := w.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		// _, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (w *IngestWorker) handleSuccess(ctx context.Context, tableName string, msgs []parsedMsg) {
	// Pre-allocate the set using the length of msgs (+1 for tableName itself) to prevent expensive rehashing
	seenSubjects := make(map[string]struct{}, len(msgs)+1)
	versionKeys := make([]string, 0, len(msgs)+1)

	encodedTable := query.SafeEncodeNATS(tableName)
	seenSubjects[encodedTable] = struct{}{}
	versionKeys = append(versionKeys, encodedTable)

	for _, pm := range msgs {
		if _, exists := seenSubjects[pm.natsSafeSubject]; !exists {
			seenSubjects[pm.natsSafeSubject] = struct{}{}
			versionKeys = append(versionKeys, pm.natsSafeSubject)
		}
	}

	if len(versionKeys) > 0 {
		invCtx := trace.ContextWithSpanContext(context.WithoutCancel(ctx), trace.SpanContextFromContext(ctx))
		_, err := w.cache.InvalidateCache(invCtx, versionKeys)
		if err != nil {
			w.logger.ErrorContext(invCtx, "failed to invalidate cache after insert - your cache is holding stale data now!", "table", tableName, "error", err)
		}
	}

	w.wg.Add(1) // Tell the main worker we have a background task running

	go func() {
		defer w.wg.Done()

		var ackWg sync.WaitGroup
		for _, pm := range msgs {
			ackWg.Add(1)

			go func(m jetstream.Msg) {
				defer ackWg.Done()
				if err := m.DoubleAck(context.WithoutCancel(ctx)); err != nil {
					w.logger.ErrorContext(context.WithoutCancel(ctx), "double ack failed for processed message", "error", err, "table", tableName)
				}
			}(pm.natsMsg)
		}

		ackWg.Wait()
	}()
}

func (w *IngestWorker) sendToDLQ(ctx context.Context, tableName string, pm parsedMsg, errMsg string) {
	subject := "dlq." + pm.natsSafeSubject

	msg := nats.NewMsg(subject)
	msg.Data = pm.natsMsg.Data()
	if msg.Header == nil {
		msg.Header = make(nats.Header)
	}
	msg.Header.Set("X-DLQ-Table", tableName)
	msg.Header.Set("X-DLQ-Error", errMsg)
	msg.Header.Set("X-DLQ-Timestamp", time.Now().UTC().Format(time.RFC3339))

	_, pubErr := w.js.PublishMsg(ctx, msg)
	if pubErr != nil {
		w.logger.ErrorContext(ctx, "NATS DLQ publish failed", "subject", subject, "error", pubErr)
	}

	// DoubleAck original message so NATS doesn't redeliver the corrupt data
	_ = pm.natsMsg.DoubleAck(ctx)
}

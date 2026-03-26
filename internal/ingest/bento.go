package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	_ "github.com/warpstreamlabs/bento/public/components/all"
	"github.com/warpstreamlabs/bento/public/service"
)

type jsInput struct {
	consumer jetstream.Consumer
	iter     jetstream.MessagesContext
	chConn   driver.Conn
	inFlight atomic.Int32
}

func (j *jsInput) Connect(ctx context.Context) error {
	iter, err := j.consumer.Messages()
	if err != nil {
		return fmt.Errorf("create jetstream iterator: %w", err)
	}
	j.iter = iter
	return nil
}

func (j *jsInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	for {
		m, err := j.iter.Next()
		if err != nil {
			return nil, nil, service.ErrNotConnected
		}

		var raw struct {
			Action    string `json:"action"`
			TableName string `json:"table_name"`
			ID        string `json:"id"`
		}
		_ = json.Unmarshal(m.Data(), &raw)

		// Delete case
		if raw.Action == "delete" {
			slog.Info("DELETE DETECTED: Pausing ingestion to flush buffer...", "table", raw.TableName, "id", raw.ID)

			ticker := time.NewTicker(10 * time.Millisecond)
			for j.inFlight.Load() > 0 {
				select {
				case <-ctx.Done():
					ticker.Stop()
					m.Nak() 
					return nil, nil, ctx.Err()
				case <-ticker.C:
				}
			}
			ticker.Stop()

			delQuery := fmt.Sprintf("DELETE FROM %s WHERE id = ?", raw.TableName)

			if err := j.chConn.Exec(ctx, delQuery, raw.ID); err != nil {
				slog.Error("Failed lightweight delete", "table", raw.TableName, "id", raw.ID, "error", err)
				m.Nak()
			} else {
				slog.Info("Successfully deleted record", "table", raw.TableName, "id", raw.ID)
				m.Ack()
			}
			continue
		}

		// Insert case
		msg := service.NewMessage(m.Data())
		
		j.inFlight.Add(1)

		ackFn := func(ctx context.Context, err error) error {
			j.inFlight.Add(-1)
			if err != nil {
				return m.Nak()
			}
			return m.Ack()
		}

		return msg, ackFn, nil
	}
}

func (j *jsInput) Close(ctx context.Context) error {
	if j.iter != nil {
		j.iter.Stop()
	}
	return nil
}

type dlqOutput struct {
	js     jetstream.JetStream
	logger *slog.Logger
}

func (d *dlqOutput) Connect(ctx context.Context) error { return nil }
func (d *dlqOutput) Wait(ctx context.Context) error    { return nil }
func (d *dlqOutput) Close(ctx context.Context) error   { return nil }
func (d *dlqOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	for _, m := range batch {
		data, _ := m.AsBytes()
		
		tableName, exists := m.MetaGet("table_name")
		
		subject := "dlq.unknown"
		if exists && tableName != "" {
			subject = "dlq." + tableName
		}
		
		_, _ = d.js.Publish(ctx, subject, data)
		d.logger.Warn("Sent failed message to DLQ", "subject", subject)
	}
	return nil
}


func StartIngestWorker(nc *nats.Conn, chConn driver.Conn, chHost, chHTTPPort, chUser, chPassword, chDB string) {
	host, _, err := net.SplitHostPort(chHost)
	if err != nil {
		host = chHost
	}

	logger := slog.Default().With("component", "bento")

	js, err := jetstream.New(nc)
	if err != nil {
		logger.Error("failed to initialize JetStream", "error", err)
		os.Exit(1)
	}

	cons, err := js.CreateOrUpdateConsumer(context.Background(), "WAVEHOUSE", jetstream.ConsumerConfig{
		Durable:       "buffer-consumer",
		FilterSubject: "ingest.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		logger.Error("failed to create durable pull consumer", "error", err)
		os.Exit(1)
	}

	if err := service.RegisterInput("nats_bridge", service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return &jsInput{
				consumer: cons,
				chConn:   chConn,
			}, nil
		},
	); err != nil {
		logger.Error("failed to register Bento input", "error", err)
		os.Exit(1)
	}

	if err := service.RegisterBatchOutput("nats_dlq_bridge", service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			return &dlqOutput{
				js:     js,
				logger: logger,
			}, service.BatchPolicy{}, 1, nil
		},
	); err != nil {
		logger.Error("failed to register Bento DLQ output", "error", err)
		os.Exit(1)
	}

	yamlConfig := fmt.Sprintf(`
input:
  nats_bridge: {}
output:
  fallback:
    - http_client:
        url: 'http://%s:%s/?database=%s&query=INSERT+INTO+${! meta("table_name") }+FORMAT+JSONEachRow&input_format_skip_unknown_fields=1&date_time_input_format=best_effort'
        verb: POST
        headers:
          Content-Type: application/json
          X-ClickHouse-User: "%s"
          X-ClickHouse-Key: "%s"
        batching:
          count: 500
          period: 5s
          processors:
            - group_by_value:
                value: '${! json("table_name") }'
            - mapping: |
                meta table_name = this.table_name
                root = this.data | {}
                root.received_timestamp = this.received_timestamp
            - archive:
                format: lines
    - nats_dlq_bridge: {}
`, host, chHTTPPort, chDB, chUser, chPassword)

	builder := service.NewStreamBuilder()
	builder.SetLogger(logger)

	if err := builder.SetYAML(yamlConfig); err != nil {
		logger.Error("Bento config error", "error", err)
		os.Exit(1)
	}

	stream, err := builder.Build()
	if err != nil {
		logger.Error("Bento build error", "error", err)
		os.Exit(1)
	}

	go func() {
		logger.Info("ingest worker started")
		if err := stream.Run(context.Background()); err != nil {
			logger.Error("ingest worker stopped", "error", err)
		}
	}()
}
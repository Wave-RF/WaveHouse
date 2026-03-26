package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	_ "github.com/warpstreamlabs/bento/public/components/all"
	"github.com/warpstreamlabs/bento/public/service"
)

// jsInput is a custom Bento input that pulls directly from a JetStream consumer.
// The consumer is captured via closure at registration time — no package-level state.
type jsInput struct {
	consumer jetstream.Consumer
	iter     jetstream.MessagesContext
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
	m, err := j.iter.Next()
	if err != nil {
		return nil, nil, service.ErrNotConnected
	}

	msg := service.NewMessage(m.Data())

	// Bento calls this AFTER the output (HTTP request to ClickHouse) finishes.
	ackFn := func(ctx context.Context, err error) error {
		if err != nil {
			return m.Nak()
		}
		return m.Ack()
	}

	return msg, ackFn, nil
}

func (j *jsInput) Close(ctx context.Context) error {
	if j.iter != nil {
		j.iter.Stop()
	}
	return nil
}

func StartIngestWorker(nc *nats.Conn, chHost, chHTTPPort, chUser, chPassword, chDB string) {
	host, _, err := net.SplitHostPort(chHost)
	if err != nil {
		host = chHost // If there was no port to begin with, just use the original
	}

	logger := slog.Default().With("component", "bento")

	// Initialize modern JetStream context.
	js, err := jetstream.New(nc)
	if err != nil {
		logger.Error("failed to initialize JetStream", "error", err)
		os.Exit(1)
	}

	// Create or bind to the Durable Pull Consumer.
	// The Sweeper relies on "buffer-consumer" to read the AckFloor.
	// TODO: support multiple consumers for horizontal scaling
	cons, err := js.CreateOrUpdateConsumer(context.Background(), "BEACHHOUSE", jetstream.ConsumerConfig{
		Durable:       "buffer-consumer",
		FilterSubject: "ingest.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		logger.Error("failed to create durable pull consumer", "error", err)
		os.Exit(1)
	}

	// Register a custom Bento input that pulls directly from JetStream.
	// The consumer is captured in the closure — no package-level state needed.
	// RegisterInput is just a registry map write; it only needs to happen
	// before builder.Build().
	if err := service.RegisterInput(
		"nats_bridge",
		service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return &jsInput{consumer: cons}, nil
		},
	); err != nil {
		logger.Error("failed to register Bento input", "error", err)
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
    - file:
        path: /tmp/bento_failed_ingest.jsonl
        codec: lines
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

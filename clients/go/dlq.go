package wavehouse

import (
	"context"
	"fmt"
	"net/url"
)

// DLQNamespace provides admin-only dead-letter-queue statistics.
//
// The server registers /v1/ops/dlq/stats only when the DLQ is enabled, so on a
// deployment with dlq.enabled: false these calls return an [*Error] with
// Status 404 — "the DLQ is switched off", not "the DLQ is empty". Check
// Status before reading a zero DLQStats as a healthy result.
type DLQNamespace struct {
	ctx          httpContext
	createStream func(table string, opts *StreamOptions) *StreamController
}

// List returns DLQ statistics (message counts per table). Admin-only.
func (d *DLQNamespace) List(ctx context.Context) (*DLQStats, error) {
	return d.stats(ctx, nil)
}

// Table returns DLQ stats filtered by table name. Admin-only.
func (d *DLQNamespace) Table(ctx context.Context, name string) (*DLQStats, error) {
	return d.stats(ctx, url.Values{"table": {name}})
}

func (d *DLQNamespace) stats(ctx context.Context, params url.Values) (*DLQStats, error) {
	var stats DLQStats
	if err := doRequest(ctx, d.ctx, requestOptions{
		method: "GET",
		path:   "/v1/ops/dlq/stats",
		params: params,
	}, &stats); err != nil {
		return nil, fmt.Errorf("get dlq stats: %w", err)
	}
	return &stats, nil
}

// Stream subscribes to live DLQ events. Not yet functional server-side (#197).
func (d *DLQNamespace) Stream(opts *StreamOptions) *StreamController {
	return d.createStream("dlq", opts)
}

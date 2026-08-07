package wavehouse

import (
	"context"
	"fmt"
	"net/url"
)

// DLQNamespace provides admin-only dead-letter-queue statistics.
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
		path:   "/v1/dlq/stats",
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

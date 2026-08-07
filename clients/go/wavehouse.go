// Package wavehouse is the official Go SDK for WaveHouse — a schema-aware
// real-time API gateway for ClickHouse. Zero third-party runtime dependencies.
//
// Create a client with [NewClient], then use [Client.From] for table
// operations, [Client.Pipe] for named queries, or the admin namespaces
// ([Client.Schema], [Client.Policy], etc.) for management.
//
//	client := wavehouse.NewClient(wavehouse.Config{
//	    BaseURL: "http://localhost:8080",
//	})
//	rows, err := client.From("clicks").SelectAll().FetchUntyped(ctx)
package wavehouse

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

// Config configures a [Client].
type Config struct {
	// BaseURL of the WaveHouse server (e.g. "http://localhost:8080").
	BaseURL string

	// Auth provides a bearer token for authenticated requests. Called before
	// each request; return "" to skip the Authorization header. Nil means
	// unauthenticated access (the server falls back to default_role).
	Auth func(ctx context.Context) (string, error)

	// Options tunes transport behavior.
	Options *ClientOptions

	// HTTPClient overrides the default http.Client. Useful for custom TLS,
	// proxies, or test transports.
	HTTPClient *http.Client
}

// ClientOptions tunes transport behavior.
type ClientOptions struct {
	// MaxRetries is the maximum number of retry attempts for retryable errors.
	// Total attempts = MaxRetries + 1. Default: 2.
	MaxRetries int
}

// StaticToken returns an Auth function that always returns the same token.
// Convenience for cases where the token doesn't rotate.
func StaticToken(token string) func(context.Context) (string, error) {
	return func(context.Context) (string, error) { return token, nil }
}

// Client is the WaveHouse SDK entry point.
type Client struct {
	ctx httpContext

	// Schema provides admin-only schema introspection.
	Schema *SchemaNamespace
	// Policy provides admin-only access-control policy management.
	Policy *PolicyNamespace
	// DLQ provides admin-only dead-letter-queue statistics.
	DLQ *DLQNamespace
	// Sys provides system health checks.
	Sys *SysNamespace
	// Pipes provides admin-only named-pipe management.
	Pipes *PipesNamespace
}

// NewClient creates a new WaveHouse client.
func NewClient(cfg Config) *Client {
	maxRetries := 2
	if cfg.Options != nil && cfg.Options.MaxRetries >= 0 {
		maxRetries = cfg.Options.MaxRetries
	}

	hc := cfg.HTTPClient
	if hc == nil {
		// Not http.DefaultClient: it's mutable global state another package
		// could reconfigure (timeout, transport, redirects) after we're built.
		hc = &http.Client{}
	}

	c := &Client{
		ctx: httpContext{
			baseURL:    trimTrailingSlashes(cfg.BaseURL),
			auth:       cfg.Auth,
			maxRetries: maxRetries,
			httpClient: hc,
		},
	}

	c.Schema = &SchemaNamespace{ctx: c.ctx}
	c.Policy = &PolicyNamespace{ctx: c.ctx}
	c.DLQ = &DLQNamespace{ctx: c.ctx, createStream: c.createStream}
	c.Sys = &SysNamespace{ctx: c.ctx}
	c.Pipes = &PipesNamespace{ctx: c.ctx}

	return c
}

// From returns a reference to a table for queries, inserts, and streams.
func (c *Client) From(table string) *TableRef {
	return &TableRef{
		ctx:          c.ctx,
		table:        table,
		createStream: c.createStream,
	}
}

// Pipe returns a reference to a named query pipe. Pass params for the pipe's
// template parameters.
func (c *Client) Pipe(name string, params map[string]any) *PipeRef {
	return &PipeRef{
		ctx:          c.ctx,
		name:         name,
		params:       params,
		createStream: c.createStream,
	}
}

// SQL executes a raw SQL query against ClickHouse. Requires the admin role.
// The server proxies the SQL verbatim to ClickHouse's HTTP interface. Results
// are decoded into []T; use [map[string]any] for dynamic schemas.
func SQL[Row any](ctx context.Context, c *Client, query string) ([]Row, error) {
	var rows []Row
	err := doRequest(ctx, c.ctx, requestOptions{
		method: "POST",
		path:   "/v1/admin/query",
		body:   map[string]string{"sql": query},
	}, &rows)
	if err != nil {
		return nil, fmt.Errorf("sql query: %w", err)
	}
	return rows, nil
}

// createStream opens an SSE stream for the given table.
func (c *Client) createStream(table string, opts *StreamOptions) *StreamController {
	return newStreamController(c.ctx, table, opts)
}

func trimTrailingSlashes(s string) string {
	return strings.TrimRight(s, "/")
}

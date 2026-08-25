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
	"maps"
	"net/http"
	"strings"
)

// Config configures a [Client].
type Config struct {
	// BaseURL of the WaveHouse server (e.g. "http://localhost:8080").
	BaseURL string

	// Auth returns a bearer token, called once per request. Return "" to skip
	// the Authorization header; nil means unauthenticated (server default_role).
	Auth func(ctx context.Context) (string, error)

	// Options tunes transport behavior.
	Options *ClientOptions

	// HTTPClient overrides the default http.Client.
	HTTPClient *http.Client
}

// ClientOptions tunes transport behavior.
type ClientOptions struct {
	// MaxRetries is the maximum number of retry attempts for retryable errors.
	// Total attempts = MaxRetries + 1. Default: 2.
	MaxRetries int

	// Headers are sent on every REST call and SSE stream. They are applied
	// before the SDK's own headers, so Authorization, Accept, Content-Type and
	// Cache-Control win any collision; names are canonicalized by net/http.
	Headers map[string]string
}

// StaticToken returns an Auth function that always returns the same token.
func StaticToken(token string) func(context.Context) (string, error) {
	return func(context.Context) (string, error) { return token, nil }
}

// Client is the WaveHouse SDK entry point.
type Client struct {
	ctx httpContext

	Schema *SchemaNamespace
	Policy *PolicyNamespace
	DLQ    *DLQNamespace
	Sys    *SysNamespace
	Pipes  *PipesNamespace
}

// NewClient creates a new WaveHouse client.
func NewClient(cfg Config) *Client {
	maxRetries := 2
	if cfg.Options != nil && cfg.Options.MaxRetries >= 0 {
		maxRetries = cfg.Options.MaxRetries
	}

	// Copy so a later mutation of the caller's map can't reach into requests.
	var headers map[string]string
	if cfg.Options != nil && len(cfg.Options.Headers) > 0 {
		headers = maps.Clone(cfg.Options.Headers)
	}

	hc := cfg.HTTPClient
	if hc == nil {
		// Not http.DefaultClient: another package could reconfigure that
		// mutable global (timeout, transport, redirects) after we are built.
		hc = &http.Client{}
	}

	c := &Client{
		ctx: httpContext{
			baseURL:    strings.TrimRight(cfg.BaseURL, "/"),
			auth:       cfg.Auth,
			maxRetries: maxRetries,
			httpClient: hc,
			headers:    headers,
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

// SQL executes a raw SQL query against ClickHouse, decoding results into []Row.
// Requires the admin role; the server proxies the SQL verbatim.
func SQL[Row any](ctx context.Context, c *Client, query string) ([]Row, error) {
	var rows []Row
	err := doRequest(ctx, c.ctx, requestOptions{
		method: "POST",
		path:   "/v1/ops/query",
		body:   map[string]string{"sql": query},
	}, &rows)
	if err != nil {
		return nil, fmt.Errorf("sql query: %w", err)
	}
	return rows, nil
}

func (c *Client) createStream(table string, opts *StreamOptions) *StreamController {
	return newStreamController(c.ctx, table, opts)
}

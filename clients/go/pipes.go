package wavehouse

import (
	"context"
	"fmt"
	"net/url"
)

// PipesNamespace provides admin-only named-pipe management.
type PipesNamespace struct {
	ctx httpContext
}

// List returns all registered pipes. Admin-only.
func (p *PipesNamespace) List(ctx context.Context) ([]Pipe, error) {
	var pipes []Pipe
	if err := doRequest(ctx, p.ctx, requestOptions{
		method: "GET",
		path:   "/v1/ops/pipes",
	}, &pipes); err != nil {
		return nil, fmt.Errorf("list pipes: %w", err)
	}
	return pipes, nil
}

// Get returns a single pipe definition by name. Admin-only.
func (p *PipesNamespace) Get(ctx context.Context, name string) (*Pipe, error) {
	var pipe Pipe
	if err := doRequest(ctx, p.ctx, requestOptions{
		method: "GET",
		path:   "/v1/ops/pipes/" + url.PathEscape(name),
	}, &pipe); err != nil {
		return nil, fmt.Errorf("get pipe %q: %w", name, err)
	}
	return &pipe, nil
}

// Set creates or updates a pipe. Admin-only.
func (p *PipesNamespace) Set(ctx context.Context, name string, def PipeDef) error {
	if err := doRequest(ctx, p.ctx, requestOptions{
		method: "PUT",
		path:   "/v1/ops/pipes/" + url.PathEscape(name),
		body:   def,
	}, nil); err != nil {
		return fmt.Errorf("set pipe %q: %w", name, err)
	}
	return nil
}

// Delete removes a pipe by name. Admin-only.
func (p *PipesNamespace) Delete(ctx context.Context, name string) error {
	if err := doRequest(ctx, p.ctx, requestOptions{
		method: "DELETE",
		path:   "/v1/ops/pipes/" + url.PathEscape(name),
	}, nil); err != nil {
		return fmt.Errorf("delete pipe %q: %w", name, err)
	}
	return nil
}

// PipeDef is the definition body for creating/updating a pipe (Pipe minus name).
type PipeDef struct {
	SQL          string     `json:"sql"`
	Parameters   []ParamDef `json:"parameters,omitempty"`
	Description  string     `json:"description,omitempty"`
	AllowedRoles []string   `json:"allowed_roles,omitempty"`
}

// PipeRef is a reference to a named query pipe. Use Fetch to execute it.
type PipeRef struct {
	ctx          httpContext
	name         string
	params       map[string]any
	createStream func(table string, opts *StreamOptions) *StreamController
}

// Fetch executes the pipe and returns the result rows decoded into []T.
func Fetch[Row any](ctx context.Context, p *PipeRef) ([]Row, error) {
	body := p.params
	if body == nil {
		body = map[string]any{}
	}
	var rows []Row
	if err := doRequest(ctx, p.ctx, requestOptions{
		method: "POST",
		path:   "/v1/pipes/" + url.PathEscape(p.name),
		body:   body,
	}, &rows); err != nil {
		return nil, fmt.Errorf("execute pipe %q: %w", p.name, err)
	}
	return rows, nil
}

// FetchUntyped executes the pipe and returns rows as []map[string]any.
func (p *PipeRef) FetchUntyped(ctx context.Context) ([]map[string]any, error) {
	return Fetch[map[string]any](ctx, p)
}

// Stream subscribes to live events using the pipe's name as a table name. The
// pipe's SQL and params are NOT applied: where a table of that name exists the
// caller receives its raw events, and otherwise the stream stays silent. Kept
// for parity with the TypeScript SDK's PipeRef.stream(), which behaves the same
// way; both wait on a pipe-aware stream endpoint (issue #445).
func (p *PipeRef) Stream(opts *StreamOptions) *StreamController {
	return p.createStream(p.name, opts)
}

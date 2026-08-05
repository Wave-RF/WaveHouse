package wavehouse

import (
	"context"
	"net/url"
)

// PipesNamespace provides admin-only named-pipe management.
type PipesNamespace struct {
	ctx httpContext
}

// List returns all registered pipes. Admin-only.
func (p *PipesNamespace) List(ctx context.Context) ([]Pipe, error) {
	var pipes []Pipe
	if err := doRequest(p.ctx, ctx, requestOptions{
		method: "GET",
		path:   "/v1/admin/pipes",
	}, &pipes); err != nil {
		return nil, err
	}
	return pipes, nil
}

// Get returns a single pipe definition by name. Admin-only.
func (p *PipesNamespace) Get(ctx context.Context, name string) (*Pipe, error) {
	var pipe Pipe
	if err := doRequest(p.ctx, ctx, requestOptions{
		method: "GET",
		path:   "/v1/admin/pipes/" + url.PathEscape(name),
	}, &pipe); err != nil {
		return nil, err
	}
	return &pipe, nil
}

// Set creates or updates a pipe. Admin-only.
func (p *PipesNamespace) Set(ctx context.Context, name string, def PipeDef) error {
	return doRequest(p.ctx, ctx, requestOptions{
		method: "PUT",
		path:   "/v1/admin/pipes/" + url.PathEscape(name),
		body:   def,
	}, nil)
}

// Delete removes a pipe by name. Admin-only.
func (p *PipesNamespace) Delete(ctx context.Context, name string) error {
	return doRequest(p.ctx, ctx, requestOptions{
		method: "DELETE",
		path:   "/v1/admin/pipes/" + url.PathEscape(name),
	}, nil)
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
	if err := doRequest(p.ctx, ctx, requestOptions{
		method: "POST",
		path:   "/v1/pipes/" + url.PathEscape(p.name),
		body:   body,
	}, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

// FetchUntyped executes the pipe and returns rows as []map[string]any.
func (p *PipeRef) FetchUntyped(ctx context.Context) ([]map[string]any, error) {
	return Fetch[map[string]any](ctx, p)
}

// Stream opens a live event stream from the pipe's underlying query.
//
// This streams by table name, using the pipe's own name as the table — it
// only works when the pipe name is also a valid table name. This matches
// the TS SDK's PipeRef.stream(), which has the same limitation.
func (p *PipeRef) Stream(opts *StreamOptions) *StreamController {
	return p.createStream(p.name, opts)
}

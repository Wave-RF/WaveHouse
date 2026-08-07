package wavehouse

import (
	"context"
	"fmt"
)

// SchemaNamespace provides admin-only schema introspection.
type SchemaNamespace struct {
	ctx httpContext
}

// List returns all table schemas discovered from ClickHouse. Admin-only.
func (s *SchemaNamespace) List(ctx context.Context) (Schemas, error) {
	// The backend returns []TableSchema; transform to map[string]TableSchema.
	var raw []TableSchema
	if err := doRequest(ctx, s.ctx, requestOptions{
		method: "GET",
		path:   "/v1/schema",
	}, &raw); err != nil {
		return nil, fmt.Errorf("list schemas: %w", err)
	}
	schemas := make(Schemas, len(raw))
	for _, t := range raw {
		schemas[t.Name] = t
	}
	return schemas, nil
}

// Refresh forces a schema re-discovery from ClickHouse. Admin-only.
func (s *SchemaNamespace) Refresh(ctx context.Context) error {
	if err := doRequest(ctx, s.ctx, requestOptions{
		method: "POST",
		path:   "/v1/schema/refresh",
	}, nil); err != nil {
		return fmt.Errorf("refresh schema: %w", err)
	}
	return nil
}

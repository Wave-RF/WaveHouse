package wavehouse

import (
	"context"
	"fmt"
)

// PolicyNamespace provides admin-only access-control policy management.
type PolicyNamespace struct {
	ctx httpContext
}

// Get returns the current access-control policy. Admin-only.
func (p *PolicyNamespace) Get(ctx context.Context) (*Policy, error) {
	var pol Policy
	if err := doRequest(ctx, p.ctx, requestOptions{
		method: "GET",
		path:   "/v1/ops/policy",
	}, &pol); err != nil {
		return nil, fmt.Errorf("get policy: %w", err)
	}
	return &pol, nil
}

// Set replaces the entire access-control policy. Admin-only.
func (p *PolicyNamespace) Set(ctx context.Context, pol *Policy) error {
	if err := doRequest(ctx, p.ctx, requestOptions{
		method: "PUT",
		path:   "/v1/ops/policy",
		body:   pol,
	}, nil); err != nil {
		return fmt.Errorf("set policy: %w", err)
	}
	return nil
}

// Validate checks a policy without applying it (dry run). Admin-only.
func (p *PolicyNamespace) Validate(ctx context.Context, pol *Policy) (*ValidationResult, error) {
	var result ValidationResult
	if err := doRequest(ctx, p.ctx, requestOptions{
		method: "POST",
		path:   "/v1/ops/policy/validate",
		body:   pol,
	}, &result); err != nil {
		return nil, fmt.Errorf("validate policy: %w", err)
	}
	return &result, nil
}

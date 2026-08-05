package wavehouse

import "context"

// PolicyNamespace provides admin-only access-control policy management.
type PolicyNamespace struct {
	ctx httpContext
}

// Get returns the current access-control policy. Admin-only.
func (p *PolicyNamespace) Get(ctx context.Context) (*Policy, error) {
	var pol Policy
	if err := doRequest(ctx, p.ctx, requestOptions{
		method: "GET",
		path:   "/v1/admin/policy",
	}, &pol); err != nil {
		return nil, err
	}
	return &pol, nil
}

// Set replaces the entire access-control policy. Admin-only.
func (p *PolicyNamespace) Set(ctx context.Context, pol *Policy) error {
	return doRequest(ctx, p.ctx, requestOptions{
		method: "PUT",
		path:   "/v1/admin/policy",
		body:   pol,
	}, nil)
}

// Validate checks a policy without applying it (dry run). Admin-only.
func (p *PolicyNamespace) Validate(ctx context.Context, pol *Policy) (*ValidationResult, error) {
	var result ValidationResult
	if err := doRequest(ctx, p.ctx, requestOptions{
		method: "POST",
		path:   "/v1/admin/policy/validate",
		body:   pol,
	}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

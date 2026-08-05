package wavehouse

import "context"

// SysNamespace provides system health checks.
type SysNamespace struct {
	ctx httpContext
}

// Health pings the server's public /v1/health endpoint. Returns nil when the
// server is reachable and past boot, or an error describing the failure.
func (s *SysNamespace) Health(ctx context.Context) error {
	return doRequest(s.ctx, ctx, requestOptions{
		method: "GET",
		path:   "/v1/health",
	}, nil)
}

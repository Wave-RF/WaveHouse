package api

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/Wave-RF/WaveHouse/internal/auth"
	"github.com/go-chi/chi/v5"
)

// writeJSONError writes a JSON error response with the correct Content-Type
// header. Unlike http.Error, which forces Content-Type: text/plain, this
// helper guarantees every error body emitted by the API is identified as
// application/json so strict clients and SDKs can parse it consistently.
//
// The Content-Type is "application/json" without a charset parameter to
// match the success-path handlers and RFC 8259 (which does not define a
// charset for application/json — JSON is required to be UTF-8 already).
func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

// writeAuthzDenied writes the response for an authorization denial and emits a
// structured WARN (see logAuthzDenied) so a misconfigured role or policy shows
// up in the logs without having to reproduce the request. When the request
// carried a present-but-invalid token (recorded by the auth middleware), it
// fails loud — 401 with the token reason ("token expired" / "invalid token") —
// so a caller whose bad token silently fell back to the default role learns why
// it lacks access, instead of a bare "forbidden". Otherwise it's an ordinary
// 403 for the caller's resolved role.
//
// Pass the role AFTER default-role resolution so forbiddenForRole's empty-role
// message is accurate. allowedRoles is the set the gate would have accepted (a
// pipe's allowed_roles); the gates with no flat role list — the /v1/admin gate
// and the policy-evaluator paths (ingest, structured query) — pass nil.
func writeAuthzDenied(w http.ResponseWriter, r *http.Request, role string, allowedRoles []string) {
	authErr := auth.AuthErrorFromContext(r.Context())

	// reason tracks the response: a present-but-invalid token fails loud (401)
	// with the sanitized token reason; otherwise it's a 403, split so the
	// empty-role case (no token / no role claim AND no default_role) is greppable
	// apart from a concrete role that simply isn't on the allowlist.
	status, reason := http.StatusForbidden, "role not in allowed roles"
	switch {
	case authErr != nil:
		status, reason = http.StatusUnauthorized, authErr.Error()
	case role == "":
		reason = "no role and no default_role configured"
	}

	logAuthzDenied(r, reason, role, allowedRoles, status)

	if authErr != nil {
		writeJSONError(w, http.StatusUnauthorized, authErr.Error())
		return
	}
	writeJSONError(w, http.StatusForbidden, forbiddenForRole(role))
}

// logAuthzDenied emits the structured WARN for an authorization denial so
// operators see a misconfigured role or policy immediately: reason, the
// observed (pre-default-resolution) and resolved roles, the roles the gate
// would have accepted, and the matched route + method. role_observed empty with
// a non-empty role_resolved means the caller presented no role and was mapped to
// default_role; roles_allowed is populated only by the pipe gate (a pipe's
// allowed_roles) and is empty for the /v1/admin gate and the policy-evaluator
// paths (ingest, structured query).
//
// slog escapes control characters in string values, so the request-derived
// fields (route, method, role) carry no log-injection risk despite originating
// in an *http.Request scope.
func logAuthzDenied(r *http.Request, reason, resolvedRole string, allowedRoles []string, status int) {
	// Prefer the matched route template (e.g. /v1/pipes/{name}) over the raw
	// path: it keeps the field low-cardinality and avoids logging concrete path
	// params. Falls back to the path when there's no chi route context (a gate
	// exercised outside the router, e.g. in a unit test).
	route := r.URL.Path
	if rctx := chi.RouteContext(r.Context()); rctx != nil {
		if pattern := rctx.RoutePattern(); pattern != "" {
			route = pattern
		}
	}
	slog.LogAttrs(r.Context(), slog.LevelWarn, "authorization denied",
		slog.String("reason", reason),
		slog.String("role_observed", auth.RoleFromContext(r.Context())),
		slog.String("role_resolved", resolvedRole),
		slog.Any("roles_allowed", allowedRoles),
		slog.String("route", route),
		slog.String("method", r.Method),
		slog.Int("status", status),
	)
}

// forbiddenForRole returns the 403 message body for a policy/allowlist denial.
// role is the caller's effective (default-resolved) role: an empty role means
// the request had no token or no role claim AND no usable default_role, so it
// says so — "forbidden" alone is opaque for that common public-access case. A
// concrete-but-unpermitted role stays terse. A present-but-invalid token never
// reaches here; writeAuthzDenied turns that into a 401 first.
func forbiddenForRole(role string) string {
	if role == "" {
		return "forbidden: request has no role and no public default_role is configured"
	}
	return "forbidden"
}

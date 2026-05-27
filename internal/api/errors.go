package api

import (
	"encoding/json"
	"net/http"

	"github.com/Wave-RF/WaveHouse/internal/auth"
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

// writeAuthzDenied writes the response for an authorization denial. When the
// request carried a present-but-invalid token (recorded by the auth
// middleware), it fails loud — 401 with the token reason ("token expired" /
// "invalid token") — so a caller whose bad token silently fell back to the
// default role learns why it lacks access, instead of a bare "forbidden".
// Otherwise it's an ordinary 403 for the caller's resolved role.
//
// Pass the role AFTER default-role resolution so forbiddenForRole's empty-role
// message is accurate.
func writeAuthzDenied(w http.ResponseWriter, r *http.Request, role string) {
	if err := auth.AuthErrorFromContext(r.Context()); err != nil {
		writeJSONError(w, http.StatusUnauthorized, err.Error())
		return
	}
	writeJSONError(w, http.StatusForbidden, forbiddenForRole(role))
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

package testutil

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RoleCase is one row of a per-resource role-authorization matrix: a resource
// restricted to AllowedRoles, observed under a given request role.
type RoleCase struct {
	Name         string
	AllowedRoles []string
	Role         string // role placed in context when SetRole is true
	SetRole      bool   // false → no role in context at all (e.g. auth disabled)
	// WantForbidden is true when the request must be rejected with 403.
	WantForbidden bool
}

// StandardRoleMatrix is the canonical set of (AllowedRoles, observed-role) cases
// every AllowedRoles-style gate must cover. The empty/absent-role rows are the
// ones that fail open when a gate only enforces its allowlist for a non-empty
// role — the class of bug behind #159. Keeping them in one shared place means a
// handler that takes AllowedRoles without running this matrix looks obviously
// under-tested in review.
func StandardRoleMatrix() []RoleCase {
	return []RoleCase{
		{Name: "open resource, no role", AllowedRoles: nil, SetRole: false, WantForbidden: false},
		{Name: "open resource, empty role", AllowedRoles: nil, Role: "", SetRole: true, WantForbidden: false},
		{Name: "open resource, any role", AllowedRoles: nil, Role: "random_role", SetRole: true, WantForbidden: false},
		{Name: "restricted, matching role", AllowedRoles: []string{"admin"}, Role: "admin", SetRole: true, WantForbidden: false},
		{Name: "restricted, non-matching role", AllowedRoles: []string{"admin"}, Role: "viewer", SetRole: true, WantForbidden: true},
		{Name: "restricted, no role in context", AllowedRoles: []string{"admin"}, SetRole: false, WantForbidden: true},
		{Name: "restricted, empty role in context", AllowedRoles: []string{"admin"}, Role: "", SetRole: true, WantForbidden: true},
		{Name: "wildcard, any role", AllowedRoles: []string{"*"}, Role: "viewer", SetRole: true, WantForbidden: false},
		{Name: "wildcard, no role", AllowedRoles: []string{"*"}, SetRole: false, WantForbidden: false},
		{Name: "multi-role, matching", AllowedRoles: []string{"admin", "editor"}, Role: "editor", SetRole: true, WantForbidden: false},
		{Name: "multi-role, non-matching", AllowedRoles: []string{"admin", "editor"}, Role: "viewer", SetRole: true, WantForbidden: true},
		{Name: "non-admin allowlist, no role in context", AllowedRoles: []string{"service"}, SetRole: false, WantForbidden: true},
		{Name: "empty allowlist entry, empty role", AllowedRoles: []string{""}, Role: "", SetRole: true, WantForbidden: true},
	}
}

// RunRoleMatrix runs each case as a parallel subtest, building and invoking the
// handler through invoke and asserting the response matches WantForbidden. A
// forbidden case must return 403 with the standard JSON error envelope
// (`application/json`, `nosniff`, an `error` field); a permitted case must
// return neither 403 nor 404. invoke owns the resource/request wiring (and any
// panic recovery for handlers that reach a nil backend on the allowed path).
func RunRoleMatrix(t *testing.T, cases []RoleCase, invoke func(t *testing.T, tc RoleCase) *httptest.ResponseRecorder) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			rec := invoke(t, tc)
			if tc.WantForbidden {
				assert.Equal(t, http.StatusForbidden, rec.Code,
					"AllowedRoles=%v must reject role=%q (set=%v)", tc.AllowedRoles, tc.Role, tc.SetRole)
				assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
				assert.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
				var body map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), "error body must be valid JSON")
				_, hasError := body["error"]
				assert.True(t, hasError, "JSON error body should contain an 'error' field")
			} else {
				assert.NotEqual(t, http.StatusForbidden, rec.Code,
					"AllowedRoles=%v must allow role=%q (set=%v)", tc.AllowedRoles, tc.Role, tc.SetRole)
				assert.NotEqual(t, http.StatusNotFound, rec.Code)
			}
		})
	}
}

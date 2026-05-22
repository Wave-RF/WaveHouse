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
// every AllowedRoles-style gate must cover. Authorization is allowlist
// membership: the observed role must appear in AllowedRoles, the privileged
// built-in roles (admin, service) always pass, and an empty/absent role matches
// nothing — so a resource with no AllowedRoles authorizes nobody but
// admin/service. The empty/absent-role rows are the ones that fail open when a
// gate only enforces its allowlist for a non-empty role — the class of bug
// behind #159. Keeping them in one shared place means a handler that takes
// AllowedRoles without running this matrix looks obviously under-tested in review.
func StandardRoleMatrix() []RoleCase {
	return []RoleCase{
		// No allowlist: authorizes nobody but the privileged built-ins.
		{Name: "no allowlist, no role", AllowedRoles: nil, SetRole: false, WantForbidden: true},
		{Name: "no allowlist, empty role", AllowedRoles: nil, Role: "", SetRole: true, WantForbidden: true},
		{Name: "no allowlist, ordinary role", AllowedRoles: nil, Role: "viewer", SetRole: true, WantForbidden: true},
		{Name: "no allowlist, admin allowed", AllowedRoles: nil, Role: "admin", SetRole: true, WantForbidden: false},
		{Name: "no allowlist, service allowed", AllowedRoles: nil, Role: "service", SetRole: true, WantForbidden: false},
		// Restricted: exact membership; the privileged built-ins bypass the list.
		{Name: "restricted, matching role", AllowedRoles: []string{"editor"}, Role: "editor", SetRole: true, WantForbidden: false},
		{Name: "restricted, non-matching role", AllowedRoles: []string{"editor"}, Role: "viewer", SetRole: true, WantForbidden: true},
		{Name: "restricted, admin bypass", AllowedRoles: []string{"editor"}, Role: "admin", SetRole: true, WantForbidden: false},
		{Name: "restricted, no role in context", AllowedRoles: []string{"editor"}, SetRole: false, WantForbidden: true},
		{Name: "restricted, empty role in context", AllowedRoles: []string{"editor"}, Role: "", SetRole: true, WantForbidden: true},
		{Name: "multi-role, matching", AllowedRoles: []string{"editor", "analyst"}, Role: "analyst", SetRole: true, WantForbidden: false},
		{Name: "multi-role, non-matching", AllowedRoles: []string{"editor", "analyst"}, Role: "viewer", SetRole: true, WantForbidden: true},
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

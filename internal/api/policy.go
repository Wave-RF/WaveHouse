package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/policy"
	"github.com/Wave-RF/WaveHouse/internal/settings"
)

// PolicyHandler serves the adopted access-control policy read-only (the
// settings directory's policies.json is the only write path) and the
// validate-without-adopting dry run.
type PolicyHandler struct {
	Source policy.Source

	// maxRequestBytes optionally overrides the default inbound body cap
	// (maxControlBodyBytes) on the decoding path (Validate). When 0, the
	// default applies. Test-only seam (pin the cap-overflow path without
	// allocating 1 MiB per run); not a production knob. Mirrors the other
	// body-decoding handlers in this package.
	maxRequestBytes int64
}

func NewPolicyHandler(source policy.Source) *PolicyHandler {
	return &PolicyHandler{Source: source}
}

// Get returns the current access control policy.
func (h *PolicyHandler) Get(w http.ResponseWriter, r *http.Request) {
	p := h.Source()
	if p == nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"tables":{}}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(p)
}

// Validate checks a policy without saving it, through the same per-document
// gate a reload runs on policies.json (settings.ValidatePolicyDocument) — so
// the dry run rejects exactly what adoption would reject. Decoding is strict:
// a misspelled key ("eq" for "_eq") is an error here, not silently dropped
// into a filter that disables row security while /validate certifies the
// policy (#514).
func (h *PolicyHandler) Validate(w http.ResponseWriter, r *http.Request) {
	reqCap := int64(maxControlBodyBytes)
	if h.maxRequestBytes > 0 {
		reqCap = h.maxRequestBytes
	}
	r.Body = http.MaxBytesReader(w, r.Body, reqCap)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		if writeMaxBytesError(w, err, reqCap) {
			return
		}
		writeJSONError(w, http.StatusBadRequest, "read request body")
		return
	}

	// Warnings don't fail the dry run, matching adoption (warnings alone
	// never block a reload).
	var errs []string
	for _, f := range settings.ValidatePolicyDocument(data) {
		if f.Severity != settings.SeverityError {
			continue
		}
		msg := f.Message
		if f.Path != "" {
			msg = f.Path + ": " + msg
		}
		errs = append(errs, msg)
	}
	if len(errs) > 0 {
		writeJSONError(w, http.StatusBadRequest, strings.Join(errs, "; "))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"valid": true})
}

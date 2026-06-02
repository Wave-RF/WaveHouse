package api

import (
	"encoding/json"
	"net/http"
	"runtime"
)

// VersionHandler serves the binary's build metadata at /version. The payload is
// fully static for the process lifetime (the ldflags-injected build info plus
// the Go runtime version), so it's marshaled once in NewVersionHandler and the
// cached bytes are written on each request — no per-request encoding.
type VersionHandler struct {
	payload []byte
}

// NewVersionHandler pre-marshals the build-info JSON from the ldflags-injected
// values (version, git commit, build time — see the Makefile / .goreleaser.yaml)
// plus runtime.Version(). Marshaling a struct of strings cannot fail, so the
// error is discarded.
func NewVersionHandler(version, gitCommit, buildTime string) *VersionHandler {
	payload, _ := json.Marshal(struct {
		Version   string `json:"version"`
		GitCommit string `json:"git_commit"`
		BuildTime string `json:"build_time"`
		GoVersion string `json:"go_version"`
	}{
		Version:   version,
		GitCommit: gitCommit,
		BuildTime: buildTime,
		GoVersion: runtime.Version(),
	})
	return &VersionHandler{payload: payload}
}

// Handle writes the pre-marshaled build metadata. No auth gate (wired among the
// public routes in NewRouter) — the values are non-sensitive and already in the
// startup logs.
func (h *VersionHandler) Handle(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_, _ = w.Write(h.payload)
}

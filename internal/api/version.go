package api

import (
	"encoding/json"
	"net/http"
	"runtime"
)

type VersionHandler struct {
	Version   string
	GitCommit string
	BuildTime string
}

// NewVersionHandler builds a VersionHandler from the ldflags-injected build
// info. Empty values are passed through verbatim — a binary built without the
// ldflags reports the cmd/wavehouse fallbacks ("dev" / "unknown") that main
// holds.
func NewVersionHandler(version, gitCommit, buildTime string) *VersionHandler {
	return &VersionHandler{Version: version, GitCommit: gitCommit, BuildTime: buildTime}
}

// Handle responds with the build metadata as a small JSON document
// ({version, git_commit, build_time, go_version}).
func (h *VersionHandler) Handle(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_ = json.NewEncoder(w).Encode(struct {
		Version   string `json:"version"`
		GitCommit string `json:"git_commit"`
		BuildTime string `json:"build_time"`
		GoVersion string `json:"go_version"`
	}{
		Version:   h.Version,
		GitCommit: h.GitCommit,
		BuildTime: h.BuildTime,
		GoVersion: runtime.Version(),
	})
}

// Package tests holds WaveHouse's Go integration tests. The shared
// TestMain harness lives in setup_test.go; per-area tests live in
// dlq_test.go, ingest_test.go, etc. Every test file is gated behind
// `//go:build integration` and the suite is invoked via
// `make test-integration`.
//
// This file has no build tag deliberately. It exists so gopls and other
// tooling have a stable package to load even when the integration tag
// isn't active in the build context — without it, a workspace pointed at
// this directory sees no Go files in the default build configuration and
// emits "No packages found" diagnostics.
//
// To make IDEs show the test files themselves, configure gopls with
// `-tags=integration`. VS Code's .vscode/settings.json already does this;
// other editors (Cursor, neovim, etc.) need the equivalent in their
// gopls config (e.g. `lua: settings.gopls.buildFlags = {"-tags=integration"}`).
package tests

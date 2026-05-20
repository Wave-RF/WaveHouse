#!/usr/bin/env bash
# PostToolUse hook: auto-format Go files with gofumpt + goimports after edits.
#
# Why a hook (vs running `make fix` periodically): catches the format drift
# at write time so commits never carry fmt-failing files, and works in
# bypassPermissions mode where teammates might not see prompts for `make fix`.
#
# Safety: best-effort. If gofumpt can't parse the file (mid-edit syntax error),
# it leaves the file alone — we silently skip rather than blocking the edit.

set -uo pipefail

input=$(cat)
file_path=$(echo "$input" | jq -r '.tool_input.file_path // empty' 2>/dev/null)

# Only Go files
[[ "$file_path" == *.go ]] || exit 0
[ -f "$file_path" ] || exit 0

cd "${CLAUDE_PROJECT_DIR:-.}" 2>/dev/null || exit 0

go tool gofumpt -w "$file_path" 2>/dev/null || true
go tool goimports -w "$file_path" 2>/dev/null || true

exit 0

// Package chversion is the one ClickHouse version this repository tests
// against. The integration suite, the E2E orchestrator and the type-layer
// tests run it, and CI and scripts/fetch-chtypes.sh fetch the chtypes artifact
// for its line, so bumping Test bumps the artifact line too.
package chversion

import "strings"

// Test is the clickhouse/clickhouse-server tag every suite runs.
// scripts/fetch-chtypes.sh reads it from this line.
const Test = "26.8.15.10"

// TestImage is Test's container image.
const TestImage = "clickhouse/clickhouse-server:" + Test

// Line is a version's major.minor line: "26.8.15.10" is "26.8".
func Line(version string) string {
	major, rest, _ := strings.Cut(version, ".")
	minor, _, _ := strings.Cut(rest, ".")
	return major + "." + minor
}

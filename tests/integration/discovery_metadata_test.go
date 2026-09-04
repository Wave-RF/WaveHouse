//go:build integration

package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
)

// TestDiscovery_MetadataAgainstRealClickHouse pins the four pieces of column and
// table metadata this branch started reading, against a live server rather than
// against the hand-authored fake in the unit tests — where the values are ones I
// chose, so they prove the plumbing and not the model of ClickHouse.
//
// `position` matters most: it is documented as 1-based in Column.Position, in
// api.md and in the SDK's Column type, and the positional wire format further up
// the stack indexes rows by it. An off-by-one there is silent data corruption,
// not a wrong number in a JSON response.
func TestDiscovery_MetadataAgainstRealClickHouse(t *testing.T) {
	table := createTable(t,
		"id UInt64, page String, note String DEFAULT 'unset', day Date MATERIALIZED toDate(now())",
		"ORDER BY id",
	)

	// createTable already refreshes the shared registry, so this reads the same
	// discovery path the rest of the suite exercises.
	reg := sharedEnv.registry
	ts := reg.Get(table)
	require.NotNil(t, ts, "the table we just created must be discovered")

	// position: 1-based and ascending in declaration order.
	require.GreaterOrEqual(t, len(ts.Columns), 4)
	assert.EqualValues(t, 1, ts.Columns[0].Position, "ClickHouse's system.columns.position is 1-based")
	for i, c := range ts.Columns {
		assert.EqualValues(t, i+1, c.Position, "column %q out of order", c.Name)
	}

	// default_expression: present for the DEFAULT column, absent for a plain one.
	byName := map[string]discovery.Column{}
	for _, c := range ts.Columns {
		byName[c.Name] = c
	}
	assert.NotEmpty(t, byName["note"].DefaultExpression, "a DEFAULT column carries its expression")
	assert.Empty(t, byName["id"].DefaultExpression, "a plain column carries none")

	// default_kind and IsInsertable land in this branch; this test covers the
	// default_expression/position half, so the MATERIALIZED column is here to
	// prove position stays contiguous across one.
	assert.EqualValues(t, 4, byName["day"].Position, "a MATERIALIZED column still occupies a position")
	// Two claims this layer makes that no unit fake can reach: testutil hardcodes
	// kind="DEFAULT" whenever HasDefault, and no unit case sets MATERIALIZED.
	// HasDefault is load-bearing — validation.go uses it to decide "not required".
	assert.True(t, byName["day"].HasDefault, "MATERIALIZED is a non-empty default_kind")
	assert.NotEmpty(t, byName["day"].DefaultExpression, "and carries its expression")

	// DDL is captured, and never serialized.
	assert.Contains(t, ts.DDL, "CREATE TABLE", "create_table_query is attached")
	assert.Contains(t, ts.DDL, table)

	// The server version probe answers something real.
	assert.NotEmpty(t, reg.ServerVersion(), "SELECT version() must populate ServerVersion")
}

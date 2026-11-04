//go:build integration

package typelayer

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// TestIngest_WindowsKeepBigMapBodiesUnderTheCut: a body over 10 MiB on a Map
// table, which one call declines (parallel-segment-cut), is accepted record
// for record in default windows.
func TestIngest_WindowsKeepBigMapBodiesUnderTheCut(t *testing.T) {
	eng := testEngine(t, mapTable())
	tbl, err := eng.Table(tenant.Default, "maps")
	require.NoError(t, err)
	defer tbl.Release()
	line := `{"n":1,"m":{"k":` + strconv.Itoa(1) + `,"padding_key_to_make_records_longer":12345}}` + "\n"
	n := (15 << 20) / len(line)
	body := []byte(strings.Repeat(line, n))

	one, err := tbl.IngestWith(FormatJSONEachRow, IngestOptions{Window: Limits{Bytes: 1 << 30, Records: 1 << 30}}, body)
	require.NoError(t, err)
	assert.Contains(t, one.Declined, "parallel", "the control: one call over the body is declined")

	b, err := tbl.IngestWith(FormatJSONEachRow, IngestOptions{}, body)
	require.NoError(t, err)
	require.Empty(t, b.Declined)
	require.Nil(t, b.Refused)
	require.Len(t, b.Rows, n)
	for i, r := range b.Rows {
		if !r.Accepted {
			t.Fatalf("record %d: %s", i+1, r.Message)
		}
	}
}

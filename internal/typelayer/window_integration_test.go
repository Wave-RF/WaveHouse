//go:build integration

package typelayer

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// TestIngest_BigMapBodyIsAcceptedWholeAndWindowed: a body over 10 MiB on a
// Map table is accepted record for record in one call and in default windows
// alike. Library builds before 20261105.095805 declined the one call
// (parallel-segment-cut), which the windows stayed under.
func TestIngest_BigMapBodyIsAcceptedWholeAndWindowed(t *testing.T) {
	eng := testEngine(t, mapTable())
	tbl, err := eng.Table(tenant.Default, "maps")
	require.NoError(t, err)
	defer tbl.Release()
	line := `{"n":1,"m":{"k":` + strconv.Itoa(1) + `,"padding_key_to_make_records_longer":12345}}` + "\n"
	n := (15 << 20) / len(line)
	body := []byte(strings.Repeat(line, n))

	for _, window := range []Limits{{Bytes: 1 << 30, Records: 1 << 30}, {}} {
		b, err := tbl.IngestWith(FormatJSONEachRow, IngestOptions{Window: window}, body)
		require.NoError(t, err)
		require.Empty(t, b.Declined, "%+v", window)
		require.Nil(t, b.Refused, "%+v", window)
		require.Len(t, b.Rows, n, "%+v", window)
		for i, r := range b.Rows {
			if !r.Accepted {
				t.Fatalf("window %+v, record %d: %s", window, i+1, r.Message)
			}
		}
	}
}

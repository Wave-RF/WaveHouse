//go:build integration

package typelayer

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// TestIngest_WindowsAcceptABigMapBody: a body over 10 MiB on a Map table,
// which one call declines on some lines and library builds (ClickHouse's
// parallel-parsing cut; measured on 26.10 build 20261105.003921, while 26.8
// accepts it since build 20261105.095805), is accepted record for record in
// default windows when its records are ordinary ones: a window holds at least
// two, so two of several MiB each are still one call.
func TestIngest_WindowsAcceptABigMapBody(t *testing.T) {
	eng := testEngine(t, mapTable())
	tbl, err := eng.Table(tenant.Default, "maps")
	require.NoError(t, err)
	defer tbl.Release()
	line := `{"n":1,"m":{"k":` + strconv.Itoa(1) + `,"padding_key_to_make_records_longer":12345}}` + "\n"
	n := (15 << 20) / len(line)
	b, err := tbl.IngestWith(FormatJSONEachRow, IngestOptions{}, []byte(strings.Repeat(line, n)))
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

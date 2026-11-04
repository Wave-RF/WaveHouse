package typelayer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// TestIngest_SuppliedIsTheRecordsOwn: Supplied is true only where the record
// gave the column its value, whatever the column stores otherwise, in every
// window, and is never asked for (always false) without the option.
func TestIngest_SuppliedIsTheRecordsOwn(t *testing.T) {
	eng := testEngine(t, &discovery.TableSchema{Name: "ids", Columns: []discovery.Column{
		{Name: "u", Type: "UUID", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "generateUUIDv4()", Position: 1},
		{Name: "n", Type: "UInt64", Position: 2},
		{Name: "k", Type: "String", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "'none'", Position: 3},
	}})
	tbl, err := eng.Table(tenant.Default, "ids")
	require.NoError(t, err)
	defer tbl.Release()

	body := []byte(`{}` + "\n" + // every column omitted
		`{"u":"00000000-0000-0000-0000-000000000000","n":0,"k":"none"}` + "\n" + // each equal to its default
		`{"u":null,"n":null,"k":null}` + "\n" + // null, read as the default
		`{"u":"61f0c404-5cb3-11e7-907b-a6006ad3dba0","n":7,"k":"x"}` + "\n")
	tests := []struct {
		column string
		want   []bool
	}{
		{"u", []bool{false, true, false, true}},
		{"n", []bool{false, true, false, true}},
		{"k", []bool{false, true, false, true}},
	}
	for _, tt := range tests {
		t.Run(tt.column, func(t *testing.T) {
			for _, lim := range []Limits{{}, {Bytes: 1 << 20, Records: 2}} {
				b, err := tbl.IngestWith(FormatJSONEachRow, IngestOptions{Supplied: tt.column, Window: lim}, body)
				require.NoError(t, err)
				require.Len(t, b.Rows, len(tt.want))
				for i, r := range b.Rows {
					require.True(t, r.Accepted, r.Message)
					assert.Equal(t, tt.want[i], r.Supplied, "record %d, window %+v", i+1, lim)
				}
			}
		})
	}

	b, err := tbl.IngestWith(FormatJSONEachRow, IngestOptions{}, body)
	require.NoError(t, err)
	for _, r := range b.Rows {
		assert.False(t, r.Supplied, "the lean document reports no provenance")
	}
}

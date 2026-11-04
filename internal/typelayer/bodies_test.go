package typelayer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wave-RF/WaveHouse/internal/typelayer"
	"github.com/Wave-RF/WaveHouse/internal/typelayer/typelayertest"
	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// TestIngest_RecoveryBodies: every body on which recovering from a bad value
// lost a record, published a fragment as a row or blamed the wrong record is
// now either accepted whole, one row per record and nothing else, or refused
// whole at the record ClickHouse failed on, with nothing accepted.
func TestIngest_RecoveryBodies(t *testing.T) {
	eng := typelayertest.TestEngine(t, typelayertest.BodyTables()...)
	for _, c := range typelayertest.RecoveryBodies {
		t.Run(string(c.Class)+"/"+c.Name, func(t *testing.T) {
			tbl, err := eng.Table(tenant.Default, c.Table)
			require.NoError(t, err)
			defer tbl.Release()
			batch, err := tbl.IngestWith(c.Format, typelayer.IngestOptions{StrictPositional: c.Strict}, []byte(c.Body))
			require.NoError(t, err)

			if c.Refused != nil {
				require.NotNil(t, batch.Refused, "refused whole, not answered per record: %+v", batch.Rows)
				assert.Empty(t, batch.Rows, "no record of a refused body is accepted")
				assert.Equal(t, c.Refused.Code, batch.Refused.Code, batch.Refused.Message)
				assert.Equal(t, c.Refused.Record, batch.Refused.Record, "the record ClickHouse failed on: %s", batch.Refused.Message)
				return
			}
			require.Nil(t, batch.Refused)
			require.Empty(t, batch.Declined)
			lines := make([]string, len(batch.Rows))
			for i, r := range batch.Rows {
				require.True(t, r.Accepted, "record %d: %s", i+1, r.Message)
				lines[i] = string(r.Line)
			}
			assert.Equal(t, c.Rows, lines, "one row per record, in order, and nothing else")
		})
	}
}

// TestIngest_WindowsRecoveryBodies reads the recovery bodies, each written to
// catch a reader that loses, splits or misnumbers a record, in small windows:
// each is still accepted whole or refused at the same record.
func TestIngest_WindowsRecoveryBodies(t *testing.T) {
	eng := typelayertest.TestEngine(t, typelayertest.BodyTables()...)
	windows := []typelayer.Limits{{Bytes: 1 << 30, Records: 2}, {Bytes: 16, Records: 1 << 30}, {Bytes: 40, Records: 3}, {Bytes: 101, Records: 5}}
	for _, c := range typelayertest.RecoveryBodies {
		t.Run(string(c.Class)+"/"+c.Name, func(t *testing.T) {
			tbl, err := eng.Table(tenant.Default, c.Table)
			require.NoError(t, err)
			defer tbl.Release()
			for _, lim := range windows {
				b, err := tbl.IngestWith(c.Format, typelayer.IngestOptions{StrictPositional: c.Strict, Window: lim}, []byte(c.Body))
				require.NoError(t, err)
				if c.Refused != nil {
					require.NotNil(t, b.Refused, "window %+v: %+v", lim, b.Rows)
					assert.Equal(t, c.Refused.Code, b.Refused.Code, "window %+v: %s", lim, b.Refused.Message)
					assert.Equal(t, c.Refused.Record, b.Refused.Record, "window %+v: %s", lim, b.Refused.Message)
					continue
				}
				require.Nil(t, b.Refused, "window %+v", lim)
				require.Empty(t, b.Declined, "window %+v", lim)
				lines := make([]string, len(b.Rows))
				for i, r := range b.Rows {
					lines[i] = string(r.Line)
				}
				assert.Equal(t, c.Rows, lines, "window %+v", lim)
			}
		})
	}
}

package typelayer

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/wave-rf/chtypes/go/v2/chtypes"

	"github.com/Wave-RF/WaveHouse/pkg/tenant"
)

// RoleShape is what a role may insert through a table: the whole input to
// Engine.RoleTable.
//
// Columns is the set of columns the role may write. nil means "every column"
// and is the identity shape. Order is irrelevant — the INSERT's column list
// follows the table's declaration order — and a name the table does not have
// is ignored. A column the role may not write is left out of the INSERT
// chtypes models and out of its grant (chtypes' insertable_columns), so a
// record naming it is refused like any field the INSERT does not list (117),
// and it holds what the server stores when an INSERT omits it: its DEFAULT (a
// generator draws, a clock reads the time), or its type's default. The export
// carries that value, so every role exports the table's whole wire row. A
// shape that leaves the role no plain or DEFAULT column to write is refused:
// no record could supply anything. An EPHEMERAL column may be supplied only if
// the role may write it (and only where inputColumns lists it).
//
// Defaults maps a column to a value used when a record omits it, or sends null
// for a non-Nullable one (chtypes' column_defaults). A value the record DOES
// supply still wins (see
// TestRoleTable_DefaultInjectsWhenAbsentAndLosesToASuppliedValue). Every key
// must name a column the role may write and must be an ordinary column (no
// DEFAULT, or a plain DEFAULT) — anything else is refused rather than silently
// reshaping the INSERT. A value is read as the record's own field would be, in
// the body's format (fieldIn); one its column cannot read refuses the call,
// as Ingest's *RoleRefused.
type RoleShape struct {
	Columns  []string
	Defaults map[string]string
}

// identity reports whether the shape asks for nothing the base table does not
// already answer, in which case RoleTable returns the base table itself.
func (s RoleShape) identity() bool {
	return s.Columns == nil && len(s.Defaults) == 0
}

// writable reports whether the role may supply column name.
func (s RoleShape) writable(name string) bool {
	return s.Columns == nil || slices.Contains(s.Columns, name)
}

// roleView is what a role's Table adds to its base table: the INSERT a body is
// modelled as for this role. The view's own inputs are the column list for a
// name-addressed body.
type roleView struct {
	// base is the table whose read lock the view holds; Release drops it.
	base *Table
	// grant is the shape's Columns, passed as insertable_columns, so chtypes
	// refuses (497) any column list wider than the role's; nil when the role
	// may write every column.
	grant []string
	// positional is the INSERT column list for a positional CSV or TSV body:
	// the plain and DEFAULT columns the role may write, in declaration order.
	// nil when the role may write every column.
	positional []string
	// defaults are the shape's Defaults, in declaration order.
	defaults []roleDefault
}

type roleDefault struct{ name, typ, value string }

// RoleTable returns one role's view of a table, read-locked exactly like
// Engine.Table: the caller must Release it, which releases the base table, and
// it stays alive and stable until then. It is the base table's lock, so the
// same rule holds: a caller holding a view must not ask for the same table
// again before releasing it.
//
// A view compiles nothing. Every call it makes runs on the base table's
// schema, with the role's shape as that call's INSERT column list and insert
// options, so ClickHouse's own parser answers the shape rather than a Go walk
// over the record's keys: a column the role may not write is one the INSERT
// does not list, so a record naming it is refused per-row with ClickHouse's
// own code 117 "Unknown field found while parsing JSONEachRow format: x"; a
// Defaults value fills a field the record omits, and a supplied one still wins.
//
// The identity shape (no column restriction, no defaults) returns the base
// table itself. A shape the table cannot express is *RoleRefused, as is a
// Defaults value its column cannot read, from Ingest. An error from resolving
// the base table itself is Table's *Unavailable.
func (e *Engine) RoleTable(id tenant.ID, table string, shape RoleShape) (*Table, error) {
	base, err := e.Table(id, table)
	if err != nil {
		return nil, err
	}
	if shape.identity() {
		return base, nil // still read-locked; the caller's Release covers it
	}
	view, err := base.view(shape)
	if err != nil {
		base.Release()
		return nil, &RoleRefused{Tenant: id, Table: table, Cause: err.Error()}
	}
	return view, nil
}

// view builds the role's view of t. The caller holds t's read lock, which the
// view's Release drops.
func (t *Table) view(shape RoleShape) (*Table, error) {
	v := &roleView{base: t}
	for _, c := range t.c.desc {
		value, inject := shape.Defaults[c.Name]
		if !inject {
			continue
		}
		switch {
		case c.DefaultKind == chtypes.KindMaterialized || c.DefaultKind == chtypes.KindAlias || c.DefaultKind == chtypes.KindEphemeral:
			return nil, fmt.Errorf("cannot inject a default into column %q: it is %s", c.Name, c.DefaultKind)
		case !shape.writable(c.Name):
			// Dropping it would turn "force this value" into "whatever the
			// server defaults to". Fail loudly instead.
			return nil, fmt.Errorf("cannot inject a default into column %q: the role may not write it", c.Name)
		}
		v.defaults = append(v.defaults, roleDefault{name: c.Name, typ: c.Type, value: value})
	}
	for _, name := range slices.Sorted(maps.Keys(shape.Defaults)) {
		if _, ok := t.cols[name]; !ok {
			return nil, fmt.Errorf("cannot inject a default into column %q: the table does not have it", name)
		}
	}

	inputs := t.inputs
	if shape.Columns != nil {
		v.grant = shape.Columns
		v.positional = writableOf(t.WireColumns, shape)
		if len(v.positional) == 0 {
			return nil, errors.New("the role may write no plain or DEFAULT column of this table, so no record could supply a value")
		}
		inputs = v.positional
		if t.inputs != nil {
			inputs = writableOf(t.inputs, shape)
		}
	}
	return &Table{
		Name:        t.Name,
		Generation:  t.Generation,
		WireColumns: t.WireColumns,
		tenant:      t.tenant,
		inputs:      inputs,
		c:           t.c,
		cols:        t.cols,
		role:        v,
	}, nil
}

// writableOf is the names in cols the role may write, in cols' order.
func writableOf(cols []string, shape RoleShape) []string {
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		if shape.writable(c) {
			out = append(out, c)
		}
	}
	return out
}

// insertOptions are the column list and insert options a body of format is
// parsed with. With no list, a body reads WireColumns. A name-addressed body
// is parsed with the inputs list (see inputColumns); a positional CSV or TSV
// one with none on a base table and with the role's writable wire columns on
// a view, since its fields map to the list by position and an EPHEMERAL slot
// would shift every field after it.
func (t *Table) insertOptions(format Format) []chtypes.RowsOption {
	positional := format == FormatCSV || format == FormatTSV
	columns := t.inputs
	if positional {
		columns = nil
	}
	var opts []chtypes.RowsOption
	if v := t.role; v != nil {
		if v.grant != nil {
			if positional {
				columns = v.positional
			}
			opts = append(opts, chtypes.WithInsertableColumns(v.grant))
		}
		if len(v.defaults) > 0 {
			opts = append(opts, chtypes.WithColumnDefaults(v.columnDefaults(format)))
		}
	}
	if columns != nil {
		opts = append(opts, chtypes.WithColumns(columns))
	}
	return opts
}

// columnDefaults spells the role's Defaults the way a body of format spells
// those fields, which is how chtypes reads a column_defaults value.
func (v *roleView) columnDefaults(format Format) []chtypes.ColumnDefault {
	out := make([]chtypes.ColumnDefault, len(v.defaults))
	for i, d := range v.defaults {
		out[i] = chtypes.ColumnDefault{Name: d.name, Value: fieldIn(format, d.typ, d.value)}
	}
	return out
}

// fieldIn spells value as one field of type typ in a body of format: TSV's
// escaped text, or for JSON and CSV a quoted string, which every scalar type
// reads but Bool (measured on the 26.8 artifact). Bool reads only bare there.
// Bare is safe: chtypes reads the value as one field and refuses any byte
// after it.
func fieldIn(format Format, typ, value string) string {
	switch {
	case format == FormatTSV || format == FormatTSVWithNames:
		return tsvField.Replace(value)
	case boolType(typ):
		return value
	case format == FormatJSONEachRow:
		return jsonString(value)
	default:
		return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
	}
}

// tsvField escapes a TSV field.
var tsvField = strings.NewReplacer(`\`, `\\`, "\t", `\t`, "\n", `\n`, "\r", `\r`)

// jsonString quotes value as a JSON string byte for byte, where encoding/json
// would replace invalid UTF-8; ClickHouse's reader keeps the bytes as sent.
func jsonString(value string) string {
	var b strings.Builder
	b.Grow(len(value) + 2)
	b.WriteByte('"')
	for i := range len(value) {
		switch c := value[i]; {
		case c == '"' || c == '\\':
			b.WriteByte('\\')
			b.WriteByte(c)
		case c < 0x20:
			fmt.Fprintf(&b, `\u%04x`, c)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// boolType reports whether typ is Bool, through Nullable and LowCardinality.
func boolType(typ string) bool {
	for {
		inner, ok := strings.CutPrefix(typ, "Nullable(")
		if !ok {
			inner, ok = strings.CutPrefix(typ, "LowCardinality(")
		}
		if !ok {
			return typ == "Bool"
		}
		typ = strings.TrimSuffix(inner, ")")
	}
}

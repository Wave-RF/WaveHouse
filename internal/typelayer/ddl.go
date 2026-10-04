package typelayer

import (
	"errors"
	"fmt"
	"strings"

	"github.com/wave-rf/chtypes/go/chtypes"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
)

// neutralEngine is the engine every CREATE TABLE compiled here declares: a
// plain MergeTree with an empty sorting key, so no merge or TTL drops a row.
// The verdicts this package asks for are the column types' and expressions',
// never the server table's engine. chtypes declines Memory ("engine not
// modelled").
const neutralEngine = "MergeTree ORDER BY tuple()"

// compiledTableName is the name every compiled table takes. Nothing reads it.
const compiledTableName = "t"

// colDecl is one column declaration: the server's own type and expression
// text, verbatim from discovery.
type colDecl struct {
	Name, Type, DefaultKind, DefaultExpression string
	// origin is who wrote DefaultExpression, for the zone rule (zoneCause).
	origin exprOrigin
}

// exprOrigin is who wrote a declaration's expression.
type exprOrigin uint8

const (
	exprServer      exprOrigin = iota // the server's own expression
	exprTypeDefault                   // defaultValueOfTypeName, for a column a role may not write
	exprLiteral                       // a literal a role shape injects
)

// declsOf is a table's discovered columns as declarations.
func declsOf(src []discovery.Column) []colDecl {
	cols := make([]colDecl, 0, len(src))
	for _, c := range src {
		cols = append(cols, colDecl{Name: c.Name, Type: c.Type, DefaultKind: c.DefaultKind, DefaultExpression: c.DefaultExpression})
	}
	return cols
}

// createTable spells cols as one CREATE TABLE statement on the neutral engine.
// It is a spelling exercise, not a semantic one: types and expressions pass
// through verbatim and names are quoted by the library's own QuoteIdentifier.
// system.columns reports the table as stored, so a Nested column arrives
// flattened (`n.a`, `n.b` Arrays) and declares as exactly those.
func createTable(lib *chtypes.Library, cols []colDecl) (string, error) {
	if len(cols) == 0 {
		return "", errors.New("no columns to declare")
	}
	var b strings.Builder
	b.WriteString("CREATE TABLE " + compiledTableName + " (")
	for i, c := range cols {
		if c.Name == "" || c.Type == "" {
			return "", fmt.Errorf("column %d has no name or type", i+1)
		}
		if i > 0 {
			b.WriteString(", ")
		}
		quoted, err := lib.QuoteIdentifier(c.Name)
		if err != nil {
			return "", fmt.Errorf("cannot quote column %q: %w", c.Name, err)
		}
		b.WriteString(quoted)
		b.WriteByte(' ')
		b.WriteString(c.Type)
		switch c.DefaultKind {
		case "":
			if c.DefaultExpression != "" {
				return "", fmt.Errorf("column %q has a default expression but no default kind", c.Name)
			}
		case "DEFAULT", "MATERIALIZED", "ALIAS":
			if c.DefaultExpression == "" {
				return "", fmt.Errorf("column %q is %s but has no expression", c.Name, c.DefaultKind)
			}
			b.WriteString(" " + c.DefaultKind + " " + c.DefaultExpression)
		case "EPHEMERAL":
			b.WriteString(" EPHEMERAL")
			if c.DefaultExpression != "" {
				b.WriteString(" " + c.DefaultExpression)
			}
		default:
			return "", fmt.Errorf("column %q has unknown default kind %q", c.Name, c.DefaultKind)
		}
	}
	b.WriteString(") ENGINE = " + neutralEngine)
	return b.String(), nil
}

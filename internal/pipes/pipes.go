package pipes

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// NamedQuery is a pre-defined SQL template with parameter support.
type NamedQuery struct {
	Name         string     `json:"name"`
	SQL          string     `json:"sql"`
	Parameters   []ParamDef `json:"parameters,omitempty"`
	Description  string     `json:"description,omitempty"`
	AllowedRoles []string   `json:"allowed_roles,omitempty"` // empty = admin role only (fails closed)
}

// ParamDef describes a query parameter.
type ParamDef struct {
	Name string `json:"name"`
	// Type documents the expected value kind ("string", "number", "boolean",
	// "array") for callers and the SDK. It is advisory only — binding keys off
	// the runtime value, and ClickHouse validates against the column type.
	Type     string `json:"type"`
	Required bool   `json:"required,omitempty"`
	Default  any    `json:"default,omitempty"`
}

// Source yields the named queries a request may run, read per call so a
// settings-directory reload applies to the next request. In production the
// Source is settings.Store (pipes.json); Static serves tests.
type Source interface {
	// Pipe returns the named query, or nil when none is defined.
	Pipe(name string) *NamedQuery
	// Pipes returns every named query.
	Pipes() []*NamedQuery
}

// Static returns a fixed in-memory Source. Intended for tests.
func Static(queries ...*NamedQuery) Source {
	m := make(map[string]*NamedQuery, len(queries))
	for _, q := range queries {
		m[q.Name] = q
	}
	return staticSource(m)
}

type staticSource map[string]*NamedQuery

func (s staticSource) Pipe(name string) *NamedQuery { return s[name] }

func (s staticSource) Pipes() []*NamedQuery {
	out := make([]*NamedQuery, 0, len(s))
	for _, q := range s {
		out = append(out, q)
	}
	return out
}

// inlineParamRe matches {{name}} or {{name:default}} placeholders in SQL templates.
var inlineParamRe = regexp.MustCompile(`\{\{(\w+)(?::([^}]*))?\}\}`)

// numericLiteralRe matches a finite base-10 numeric literal that both Go and
// ClickHouse accept: optional sign, integer/decimal mantissa, optional exponent.
// It deliberately excludes the Go-only spellings strconv.ParseFloat would accept
// but ClickHouse cannot parse — underscore separators ("1_000"), hex floats
// ("0x1p-2"), and the non-finite "Inf"/"NaN".
var numericLiteralRe = regexp.MustCompile(`^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$`)

// isNumericLiteral reports whether s is a plain SQL numeric literal. A
// query-string parameter value (always a string) renders bare only when it
// matches — any other string is quoted, so a numeric-looking value can never
// become invalid bare SQL.
func isNumericLiteral(s string) bool {
	return numericLiteralRe.MatchString(s)
}

// BindParams replaces {{param}} and {{param:default}} placeholders in a NamedQuery's SQL
// with supplied values or defaults. Formally declared Parameters provide type info and
// required/default metadata. Inline {{name:default}} syntax also works without formal
// parameter definitions.
//
// Values are inlined directly into the SQL string (scalars are escaped, arrays
// render as a parenthesized list — see formatParamValue). This avoids
// driver-level positional parameter limitations (e.g. LIMIT position).
func BindParams(q *NamedQuery, supplied map[string]any) (string, []any, error) {
	// Build lookup from formal parameter definitions.
	formal := make(map[string]*ParamDef, len(q.Parameters))
	for i := range q.Parameters {
		formal[q.Parameters[i].Name] = &q.Parameters[i]
	}

	// Pre-check: all required formal params must be supplied (even if not in SQL).
	for _, p := range q.Parameters {
		if p.Required {
			if _, ok := supplied[p.Name]; !ok {
				return "", nil, fmt.Errorf("missing required parameter: %s", p.Name)
			}
		}
	}

	// Single pass: find all {{name}} / {{name:default}} placeholders and resolve each.
	var bindErr error

	sql := inlineParamRe.ReplaceAllStringFunc(q.SQL, func(match string) string {
		if bindErr != nil {
			return match
		}
		sub := inlineParamRe.FindStringSubmatch(match)
		name := sub[1]
		inlineDefault := sub[2] // empty string if no :default

		var val any
		if v, ok := supplied[name]; ok {
			val = v
		} else if p, ok := formal[name]; ok {
			val = p.Default
		} else if inlineDefault != "" {
			val = inlineDefault
		} else {
			bindErr = fmt.Errorf("missing required parameter: %s", name)
			return match
		}

		lit, err := formatParamValue(val)
		if err != nil {
			bindErr = fmt.Errorf("parameter %q: %w", name, err)
			return match
		}
		return lit
	})

	if bindErr != nil {
		return "", nil, bindErr
	}
	return sql, nil, nil
}

// formatParamValue converts a Go value to a safe SQL literal for inline
// substitution. Scalars are escaped (strings single-quoted with `'` and `\`
// doubled, numbers emitted bare). A JSON array becomes a parenthesized,
// comma-separated list of recursively formatted elements — the `(v1, v2, …)`
// shape ClickHouse expects on the right of `IN`, matching how the
// structured-query builder renders an IN clause. Because every scalar leaf is
// escaped, no value — or array element — can break out of its literal.
//
// Values with no scalar SQL representation are refused rather than emitted as
// Go's `%v` text: a JSON object has no meaning here, and an empty array would
// render as `IN ()`, a ClickHouse syntax error.
func formatParamValue(v any) (string, error) {
	if v == nil {
		return "NULL", nil
	}
	switch val := v.(type) {
	case string:
		// A numeric-looking string renders bare (so `?limit=100` from a query
		// string works as a number); any other string is quoted and escaped.
		if isNumericLiteral(val) {
			return val, nil
		}
		escaped := strings.ReplaceAll(val, `\`, `\\`)
		escaped = strings.ReplaceAll(escaped, `'`, `''`)
		return "'" + escaped + "'", nil
	case float64:
		// JSON numbers are float64; if it's a whole number, format as integer.
		if val == float64(int64(val)) {
			return strconv.FormatInt(int64(val), 10), nil
		}
		return fmt.Sprintf("%g", val), nil
	case float32:
		if val == float32(int32(val)) {
			return fmt.Sprintf("%d", int32(val)), nil
		}
		return fmt.Sprintf("%g", val), nil
	case bool:
		if val {
			return "1", nil
		}
		return "0", nil
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", val), nil
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val), nil
	case []any:
		if len(val) == 0 {
			return "", fmt.Errorf("array parameter must not be empty")
		}
		parts := make([]string, len(val))
		for i, elem := range val {
			s, err := formatParamValue(elem)
			if err != nil {
				return "", err
			}
			parts[i] = s
		}
		return "(" + strings.Join(parts, ", ") + ")", nil
	default:
		return "", fmt.Errorf("unsupported parameter type %s", jsonKind(v))
	}
}

// jsonKind names the JSON shape of a decoded value for error messages.
func jsonKind(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "number"
	case string:
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return fmt.Sprintf("%T", v)
	}
}

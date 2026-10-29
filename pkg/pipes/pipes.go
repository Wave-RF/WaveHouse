// Package pipes defines named query pipes (pipes.json): the NamedQuery
// type, its parameter definitions, and the binding of supplied values into
// the SQL template. Importable from outside the module; see pkg/settings.
package pipes

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/Wave-RF/WaveHouse/pkg/chsql"
	"github.com/Wave-RF/WaveHouse/pkg/policy"
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

// paramPattern is the parameter placeholder grammar, {{name}} or
// {{name:default}}: group 1 the name, group 2 the inline default. A name is
// word characters only, so a claim reference ({{ jwt.<path> }}, with its dot)
// can never read as a parameter, and a parameter value can never stand in
// for a claim.
const paramPattern = `\{\{(\w+)(?::([^}]*))?\}\}`

// placeholderRe matches every placeholder of a SQL template in one pass: a
// claim reference first (group 1 its path, policy.ClaimTemplatePattern), else
// a parameter placeholder (groups 2 and 3). One regexp, one
// ReplaceAllStringFunc, so text a placeholder binds to is never rescanned: a
// value that happens to contain `{{` is inert.
var placeholderRe = regexp.MustCompile(policy.ClaimTemplatePattern + `|` + paramPattern)

// paramRe matches a parameter placeholder alone, for CheckTemplate.
var paramRe = regexp.MustCompile(paramPattern)

// ReservedParam is the parameter name no pipe may declare or reference:
// {{ jwt.<path> }} reads a token claim, and a parameter of that name beside
// it would read as a request-supplied claim. CheckTemplate refuses it inline
// ({{jwt}}); the settings validator refuses it declared.
const ReservedParam = "jwt"

// malformedRe matches a `{{` that starts like a placeholder — optional
// spaces, then a letter, digit, `_` or `.` — which, once every well-formed
// placeholder is stripped, is one BindParams would leave as literal text. A
// `{{` followed by anything else — ClickHouse's own brace escape in a
// format() string, a `'{{'` literal — is no placeholder attempt and is left
// to ClickHouse.
var malformedRe = regexp.MustCompile(`\{\{\s*[\w.]`)

// UnresolvedClaimError is BindParams's error for a `{{ jwt.<path> }}`
// reference the caller's token cannot bind (policy.ResolveClaim): the
// handler fails the request closed, never binds a stand-in.
type UnresolvedClaimError struct {
	// Path is the claim path, as written in the template.
	Path string
}

func (e *UnresolvedClaimError) Error() string {
	return fmt.Sprintf("claim %q cannot be resolved from the token", e.Path)
}

// Claims returns the claim paths the pipe's SQL binds through
// `{{ jwt.<path> }}` references, each once, in order of first use; nil for a
// pipe that binds none. It is BindParams's own scan (placeholderRe), so a
// claim-shaped fragment inside a parameter's inline default, which binds the
// parameter, is not reported. Validation reports the paths so a reviewer
// sees that a pipe is claim-scoped.
func (q *NamedQuery) Claims() []string {
	var paths []string
	seen := map[string]bool{}
	for _, m := range placeholderRe.FindAllStringSubmatch(q.SQL, -1) {
		if path := m[1]; path != "" && !seen[path] {
			seen[path] = true
			paths = append(paths, path)
		}
	}
	return paths
}

// Skeleton returns the SQL template with every placeholder bound to an empty
// string literal, two single quotes — the shape a bound value has (a quoted
// literal, a number, a parenthesized list) — so a classifier that must step
// over a value reads the statement a call would run, with no parameters or
// claims in hand: IsMutation's `EXECUTE AS <user>` slot, or a quoted inline
// default in a WITH list, would otherwise hide the verb behind a `{{`.
func (q *NamedQuery) Skeleton() string {
	return placeholderRe.ReplaceAllString(q.SQL, "''")
}

// CheckTemplate reports the first thing wrong with the SQL template's
// placeholders, nil when there is none: an inline placeholder naming the
// reserved parameter (`{{jwt}}`); a placeholder inside a comment or a quoted
// token, where ClickHouse reads text but BindParams binds — in a comment the
// value is bound, and a claim gates the pipe, while nothing that runs uses
// it; inside quotes the bound value's own quotes close the template's and
// the value runs as SQL; or a `{{` that starts like a placeholder but is
// neither a parameter placeholder nor a well-formed claim reference — a
// claim path outside the grammar (`{{ jwt.tenant-id }}`), a space around a
// parameter's name, an unterminated `{{`. BindParams would send such a
// fragment to ClickHouse as literal text, so validation rejects it on every
// adoption instead. A declared parameter named ReservedParam is the settings
// validator's own check, filed at the parameter.
func (q *NamedQuery) CheckTemplate() error {
	for _, m := range paramRe.FindAllStringSubmatch(q.SQL, -1) {
		if m[1] == ReservedParam {
			return fmt.Errorf("placeholder %q names the reserved parameter %q — {{ jwt.<path> }} reads a claim of the caller's token", m[0], ReservedParam)
		}
	}
	// Where ClickHouse reads text, BindParams still binds: chsql.Opaque finds
	// the comments and quoted tokens. Every placeholder is masked by spaces of
	// its own length first, so a quote in an inline default opens no token in
	// the scan, and the positions hold.
	locs := placeholderRe.FindAllStringIndex(q.SQL, -1)
	if len(locs) > 0 {
		masked := []byte(q.SQL)
		for _, loc := range locs {
			for k := loc[0]; k < loc[1]; k++ {
				masked[k] = ' '
			}
		}
		spans := chsql.Opaque(string(masked))
		for _, loc := range locs {
			for _, span := range spans {
				if loc[0] < span.Start || loc[0] >= span.End {
					continue
				}
				if span.Comment {
					return fmt.Errorf("placeholder %q inside a comment — it is bound, and a claim gates the pipe, while nothing that runs uses it; delete the line or move the placeholder into the statement", q.SQL[loc[0]:loc[1]])
				}
				return fmt.Errorf("placeholder %q inside quotes — write it bare: a bound value brings its own quotes, and inside the template's it would run as SQL", q.SQL[loc[0]:loc[1]])
			}
		}
	}
	// Strip every well-formed placeholder; a surviving placeholder-like "{{"
	// is one the binder would leave as literal text. Stripping can splice one
	// from the fragments around a placeholder ("{{{{x}}a" strips to "{{a"), a
	// rare over-rejection that errs fail-closed at validation time, never at
	// request time.
	stripped := placeholderRe.ReplaceAllString(q.SQL, "")
	loc := malformedRe.FindStringIndex(stripped)
	if loc == nil {
		return nil
	}
	frag := stripped[loc[0]:]
	if end := strings.Index(frag, "}}"); end >= 0 {
		frag = frag[:end+2]
	}
	return fmt.Errorf("unrecognized placeholder %q — a parameter is {{name}} or {{name:default}}, a claim is {{ jwt.<path> }} with a path of letters, digits, '_' and '.'", frag)
}

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
// with supplied values or defaults, and {{ jwt.<path> }} references with the
// caller's verified token claims. Formally declared Parameters provide type info and
// required/default metadata. Inline {{name:default}} syntax also works without formal
// parameter definitions.
//
// Values are inlined directly into the SQL string (scalars are escaped, arrays
// render as a parenthesized list — see formatParamValue). This avoids
// driver-level positional parameter limitations (e.g. LIMIT position).
//
// A claim reference never reads supplied: it resolves through
// policy.ResolveClaim against claims alone — nil for a request that carried
// no valid token — and binds in canonical form as a quoted literal, a set of
// them for an array claim (`('a', 'b')`, for IN), exactly as a policy filter
// binds the same claim; ClickHouse reads a quoted constant as the column's
// type. A reference the token cannot bind returns *UnresolvedClaimError, after
// which the SQL is unusable: the caller fails closed.
func BindParams(q *NamedQuery, supplied, claims map[string]any) (string, []any, error) {
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

	// Single pass: find every placeholder and resolve each.
	var bindErr error

	sql := placeholderRe.ReplaceAllStringFunc(q.SQL, func(match string) string {
		if bindErr != nil {
			return match
		}
		sub := placeholderRe.FindStringSubmatch(match)
		if path := sub[1]; path != "" {
			vals, set, ok := policy.ResolveClaim(claims, path)
			if !ok {
				bindErr = &UnresolvedClaimError{Path: path}
				return match
			}
			return claimLiteral(vals, set)
		}
		name := sub[2]
		inlineDefault := sub[3] // empty string if no :default

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
// substitution. Scalars are escaped (strings single-quoted, see quoteLiteral;
// numbers emitted bare). A JSON array becomes a parenthesized,
// comma-separated list of recursively formatted elements — the `(v1, v2, …)`
// shape ClickHouse expects on the right of `IN`, matching how the
// structured-query builder renders an IN clause. Because every scalar leaf is
// escaped, no value — or array element — can break out of its literal, as long
// as the template writes the placeholder bare: inside quotes (`'{{id}}'`) the
// value's own quotes close the template's.
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
			return bareNumber(val), nil
		}
		return quoteLiteral(val), nil
	case float64:
		// JSON numbers are float64; if it's a whole number, format as integer.
		if val == float64(int64(val)) {
			return bareNumber(strconv.FormatInt(int64(val), 10)), nil
		}
		return bareNumber(fmt.Sprintf("%g", val)), nil
	case float32:
		if val == float32(int32(val)) {
			return bareNumber(fmt.Sprintf("%d", int32(val))), nil
		}
		return bareNumber(fmt.Sprintf("%g", val)), nil
	case bool:
		if val {
			return "1", nil
		}
		return "0", nil
	case int, int8, int16, int32, int64:
		return bareNumber(fmt.Sprintf("%d", val)), nil
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

// quoteLiteral renders s as a single-quoted SQL string literal: `'` and `\`
// doubled, a newline or carriage return written as ClickHouse's `\n` or `\r`
// escape, and every `*` or `$` as its `\x2A` or `\x24` hex escape — each the
// same value — so s can neither close the literal nor end a `--` comment a
// placeholder sits in, nor close or open (ClickHouse nests them) a `/* */`
// comment, nor close a `$tag$` heredoc the template opened: no `*` or `$` is
// left for any of those sequences. A `$tag$` with no closer is a bareword, to
// ClickHouse and to CheckTemplate alike, so the closer must never come from
// a value: it would end the heredoc at the value's own quote and run the rest
// of the value as SQL.
func quoteLiteral(s string) string {
	return "'" + literalEscaper.Replace(s) + "'"
}

// literalEscaper is quoteLiteral's one-pass escaping, so the backslash of an
// escaped newline, star or dollar is never doubled in turn.
var literalEscaper = strings.NewReplacer(`\`, `\\`, `'`, `''`, "\n", `\n`, "\r", `\r`, `*`, `\x2A`, `$`, `\x24`)

// bareNumber renders a numeric literal for inline substitution: a negative
// one with a leading space, so that bound right after a `-` in the template
// (`0-{{n}}`, `now()-{{secs}}`) it reads `0- -5`, never `0--5`, which
// ClickHouse would read as the start of a `--` line comment that drops the
// rest of the statement, a claim predicate included.
func bareNumber(s string) string {
	if strings.HasPrefix(s, "-") {
		return " " + s
	}
	return s
}

// claimLiteral renders a resolved claim (policy.ResolveClaim) for inline
// substitution: every value quoted, a set as a parenthesized list. Quoted
// even when it spells a number — unlike a request parameter, which renders a
// numeric-looking string bare — because that is how a policy filter binds the
// same claim (as a string parameter), and a claim's spelling is the token's,
// not the pipe author's: ClickHouse reads a quoted constant as the column's
// type, so `tenant_id = '42'` holds on a UInt64 column and on a String one.
func claimLiteral(vals []string, set bool) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		parts[i] = quoteLiteral(v)
	}
	if set {
		return "(" + strings.Join(parts, ", ") + ")"
	}
	return parts[0]
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

package policy

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/chsql"
)

// Policy is the top-level access control configuration.
type Policy struct {
	// DefaultRole is the role an empty/absent role resolves to (a tokenless or
	// roleless request). Empty means no public access. Setting it equal to
	// AdminRole grants every roleless request full admin — permitted as a
	// local/dev convenience, but the store warns loudly when such a policy is
	// adopted (see DefaultRoleGrantsAdmin); avoid it in production.
	DefaultRole string `json:"default_role" yaml:"default_role"`
	// AdminRole is the role granted full access and the allowlist bypass.
	// Configurable per org (case-sensitive, exact match); defaults to "admin"
	// when unset (see AdminRole()).
	AdminRole string                 `json:"admin_role,omitempty" yaml:"admin_role,omitempty"`
	Tables    map[string]TablePolicy `json:"tables" yaml:"tables"`
}

// TablePolicy defines access control for a single table.
type TablePolicy struct {
	Select map[string]RolePermissions `json:"select,omitempty" yaml:"select,omitempty"`
	Insert map[string]RolePermissions `json:"insert,omitempty" yaml:"insert,omitempty"`
}

// RolePermissions defines what a role can do on a table for a specific operation.
type RolePermissions struct {
	AllowColumns        []string          `json:"allow_columns,omitempty" yaml:"allow_columns,omitempty"`
	DenyColumns         []string          `json:"deny_columns,omitempty" yaml:"deny_columns,omitempty"`
	Filter              map[string]Filter `json:"filter,omitempty" yaml:"filter,omitempty"`
	Check               map[string]Filter `json:"check,omitempty" yaml:"check,omitempty"`
	AllowedAggregations []string          `json:"allowed_aggregations,omitempty" yaml:"allowed_aggregations,omitempty"`
	DeniedAggregations  []string          `json:"denied_aggregations,omitempty" yaml:"denied_aggregations,omitempty"`
	MaxRows             int               `json:"max_rows,omitempty" yaml:"max_rows,omitempty"`
	// MaxExecutionTime, MaxRowsToRead, and MaxMemoryUsage are enforced
	// server-side by ClickHouse (the max_execution_time / max_rows_to_read /
	// max_memory_usage settings, #316), not just as a client-side context
	// deadline. They cap wall-clock time, rows scanned, and peak query memory so
	// a heavy aggregation can't exhaust the box within the time budget.
	// MaxExecutionTime and MaxMemoryUsage are human-readable scalars ("5s",
	// "4GiB") to match clickhouse.query_timeout; MaxRowsToRead is a plain count
	// (int64, since it can exceed 2^31 on a large table).
	MaxExecutionTime Millis   `json:"max_execution_time,omitempty" yaml:"max_execution_time,omitempty"`
	MaxRowsToRead    int64    `json:"max_rows_to_read,omitempty" yaml:"max_rows_to_read,omitempty"`
	MaxMemoryUsage   ByteSize `json:"max_memory_usage,omitempty" yaml:"max_memory_usage,omitempty"`
}

// Filter represents a single comparison operation.
type Filter struct {
	Eq  *string `json:"_eq,omitempty" yaml:"_eq,omitempty"`
	Neq *string `json:"_neq,omitempty" yaml:"_neq,omitempty"`
	Gt  *string `json:"_gt,omitempty" yaml:"_gt,omitempty"`
	Lt  *string `json:"_lt,omitempty" yaml:"_lt,omitempty"`
	In  *string `json:"_in,omitempty" yaml:"_in,omitempty"`
}

// ResolvedPermissions is the result of evaluating a policy against JWT claims.
type ResolvedPermissions struct {
	Allowed      bool
	AllowColumns []string
	DenyColumns  []string
	WhereClause  string
	WhereParams  []any
	// rowFilter is the same row-level-security predicate as WhereClause/WhereParams,
	// kept in resolved form so the stream path can evaluate it in memory (RowVisible)
	// while the query path renders it to SQL. Both derive from one resolvePredicates
	// call in Evaluate, so the two read surfaces can't drift. See rowfilter.go.
	rowFilter           []resolvedPredicate
	CheckClauses        map[string]any // column → required value (for inserts)
	AllowedAggregations []string
	DeniedAggregations  []string
	MaxRows             int
	MaxExecutionTime    Millis
	MaxRowsToRead       int64
	MaxMemoryUsage      ByteSize
}

// claimTemplateRe matches {{ jwt.claim.path }} templates.
var claimTemplateRe = regexp.MustCompile(`\{\{\s*jwt\.([a-zA-Z0-9_.]+)\s*\}\}`)

// wholeClaimRe matches a value that is EXACTLY one {{ jwt.path }} reference with
// no surrounding text (anchored, unlike claimTemplateRe). It lets the _in
// resolver pull an array-valued claim through whole — col IN (its elements) —
// rather than stringifying it as resolveTemplate would.
var wholeClaimRe = regexp.MustCompile(`^\{\{\s*jwt\.([a-zA-Z0-9_.]+)\s*\}\}$`)

// ResolveRole maps an empty/absent role to the policy's default_role, so a
// request with no role — a token without a role claim, or no token at all when
// public access is configured — is evaluated as the configured default.
// Matching is exact and case-sensitive (no normalization): roles are opaque
// strings, mirroring the admin check in IsAdmin. An empty default_role (no
// public access configured) leaves the role empty so the request fails closed.
// A default_role equal to the admin role IS honored — a roleless request then
// resolves to admin and receives full access — which is permitted as a local/dev
// convenience; the store warns loudly when such a policy is adopted (see
// DefaultRoleGrantsAdmin). A non-empty role is returned unchanged — roles do not
// inherit the default's permissions.
func ResolveRole(p *Policy, role string) string {
	if role != "" || p == nil {
		return role
	}
	return p.DefaultRole
}

// Evaluate resolves a policy for a given role, table, and operation against JWT claims.
func Evaluate(p *Policy, role, table, operation string, claims map[string]any) *ResolvedPermissions {
	// Map an empty/absent role to the configured default_role (no-op if none,
	// or if the policy is nil). A non-empty role is unchanged — roles never
	// inherit the default's permissions.
	role = ResolveRole(p, role)

	// Admin bypasses all policy: full, unrestricted access regardless of table,
	// operation, or any explicit per-role entry — an admin is never column- or
	// row-restricted, even by a policy that names the admin role. This single
	// early exit is the only admin branch, so every return below is the non-admin
	// path and denies plainly. It also folds in the nil-policy case: IsAdmin(nil)
	// is false (a deleted/absent policy is a total lockout — nobody passes, not
	// even admin; a fresh deployment is bootstrapped from the policy file, not
	// over HTTP), so a nil policy falls through to the deny just below. Mirrors
	// RoleAllowed's admin short-circuit on the pipe path.
	if IsAdmin(p, role) {
		return &ResolvedPermissions{Allowed: true}
	}

	// Beyond here the role is non-admin (non-empty only if it carried a concrete
	// role or matched a real default_role); any failure to find a matching entry
	// is a plain deny.
	if p == nil {
		return &ResolvedPermissions{Allowed: false}
	}

	tp, ok := p.Tables[table]
	if !ok {
		// No policy for this table — default deny.
		return &ResolvedPermissions{Allowed: false}
	}

	var rolePerms map[string]RolePermissions
	switch operation {
	case "select":
		rolePerms = tp.Select
	case "insert":
		rolePerms = tp.Insert
	default:
		return &ResolvedPermissions{Allowed: false}
	}

	if rolePerms == nil {
		return &ResolvedPermissions{Allowed: false}
	}

	// An empty/absent role must never match a role entry — a roleless request is
	// authorized only after ResolveRole maps it to a concrete default_role above,
	// or via the admin role (handled by the short-circuit at the top). A stray ""
	// role key in a policy therefore grants nothing: the policy-side twin of the
	// empty-AllowedRoles-entry footgun closed for pipes in #159. Matching is
	// exact — there is no "*" any-role wildcard.
	perms, ok := RolePermissions{}, false
	if role != "" {
		perms, ok = rolePerms[role]
	}
	if !ok {
		return &ResolvedPermissions{Allowed: false}
	}

	resolved := &ResolvedPermissions{
		Allowed:             true,
		AllowColumns:        perms.AllowColumns,
		DenyColumns:         perms.DenyColumns,
		AllowedAggregations: perms.AllowedAggregations,
		DeniedAggregations:  perms.DeniedAggregations,
		MaxRows:             perms.MaxRows,
		MaxExecutionTime:    perms.MaxExecutionTime,
		MaxRowsToRead:       perms.MaxRowsToRead,
		MaxMemoryUsage:      perms.MaxMemoryUsage,
	}

	// Resolve filters into WHERE clause. A bind-unsafe filter column can't be
	// emitted safely — a '?' in it would shift clickhouse-go's positional value
	// binding, including this RLS filter's own bound value — so deny the role
	// fail-closed rather than drop the predicate (which would widen row access)
	// or emit a mis-bound query. validateRolePerms rejects such a policy at write
	// time; this guards the query path as defense-in-depth (Evaluate does not
	// re-validate the policy it is handed).
	if len(perms.Filter) > 0 {
		for col := range perms.Filter {
			if chsql.BindUnsafe(col) {
				return &ResolvedPermissions{Allowed: false}
			}
		}
		// Resolve the row-filter once into predicates, then render both read surfaces
		// from that single source so they can't drift: the query path binds them into
		// a SQL WHERE here; the stream path evaluates the same predicates in memory
		// (ResolvedPermissions.RowVisible).
		preds := resolvePredicates(perms.Filter, claims)
		resolved.rowFilter = preds
		clauses, params := predicatesToSQL(preds)
		if len(clauses) > 0 {
			resolved.WhereClause = strings.Join(clauses, " AND ")
			resolved.WhereParams = params
		}
	}

	// Resolve check rules (for inserts).
	if len(perms.Check) > 0 {
		resolved.CheckClauses = make(map[string]any, len(perms.Check))
		for col, f := range perms.Check {
			switch {
			case f.Eq != nil:
				// Deliberate asymmetry with the read path: an unresolvable check claim
				// still resolves to "" and is auto-injected as the required value (#463).
				// A placeholder-free value is marked LiteralValue so the check
				// comparison can accept its numeric reading; a claim-derived value
				// stays a plain string and keeps strict canonical equality.
				v, _ := resolveTemplate(*f.Eq, claims)
				if !claimTemplateRe.MatchString(*f.Eq) {
					resolved.CheckClauses[col] = LiteralValue(v)
				} else {
					resolved.CheckClauses[col] = v
				}
			case f.In != nil:
				// A []any value marks a set-membership check (vs a scalar required
				// value); ingest enforces "inserted value must be one of these".
				resolved.CheckClauses[col] = resolveInValues(*f.In, claims)
			}
		}
	}

	return resolved
}

// resolvedPredicate is one row-filter comparison with its claim templates already
// resolved to concrete string values — the shared, render-agnostic form the query
// path turns into SQL (predicatesToSQL) and the stream path evaluates in memory
// (RowVisible). Op is one of "=", "!=", ">", "<", "in". Values holds one element
// for the scalar operators and zero-or-more for "in"; an EMPTY Values matches no
// rows on either surface — an empty/unresolvable "in" set, or a scalar whose
// constant was unresolvable (an absent/null claim, a structured value, or one with
// no canonical form — see resolveTemplate/CanonicalScalar).
type resolvedPredicate struct {
	Column string
	Op     string
	Values []string
}

// resolvePredicates resolves each filter's claim templates once into predicates.
// Both read surfaces derive from this single result so they can't drift; the
// operator order within a column (=, !=, >, <, in) mirrors the former inline SQL.
func resolvePredicates(filters map[string]Filter, claims map[string]any) []resolvedPredicate {
	var preds []resolvedPredicate
	// An unresolvable constant (ok=false from resolveTemplate) yields a predicate
	// with NO values, which matches no rows on either surface (#385) — never a
	// synthesized stand-in that could match some other principal's rows.
	scalar := func(col, op, tmpl string) resolvedPredicate {
		v, ok := resolveTemplate(tmpl, claims)
		if !ok {
			return resolvedPredicate{Column: col, Op: op}
		}
		return resolvedPredicate{Column: col, Op: op, Values: []string{v}}
	}
	for col, f := range filters {
		if f.Eq != nil {
			preds = append(preds, scalar(col, "=", *f.Eq))
		}
		if f.Neq != nil {
			preds = append(preds, scalar(col, "!=", *f.Neq))
		}
		if f.Gt != nil {
			preds = append(preds, scalar(col, ">", *f.Gt))
		}
		if f.Lt != nil {
			preds = append(preds, scalar(col, "<", *f.Lt))
		}
		if f.In != nil {
			preds = append(preds, resolvedPredicate{col, "in", toStrings(resolveInValues(*f.In, claims))})
		}
	}
	return preds
}

// predicatesToSQL renders resolved predicates into WHERE clauses and bound params.
func predicatesToSQL(preds []resolvedPredicate) ([]string, []any) {
	var clauses []string
	var params []any
	for _, p := range preds {
		// Quote the policy-authored column the same way the query builder quotes
		// caller columns, so a row-filter on a weird-but-legal column name (dots,
		// spaces, keywords) is emitted safely.
		qcol := chsql.QuoteIdent(p.Column)
		switch p.Op {
		case "in":
			if len(p.Values) == 0 {
				// An empty/unresolvable set fails closed: a row filter scoped to no
				// values matches no rows, never widening to all of them (the #224
				// fail-open). `IN ()` is not valid SQL, so emit a constant false.
				clauses = append(clauses, "1 = 0")
			} else {
				placeholders := strings.TrimSuffix(strings.Repeat("?,", len(p.Values)), ",")
				clauses = append(clauses, fmt.Sprintf("%s IN (%s)", qcol, placeholders))
				for _, v := range p.Values {
					params = append(params, v)
				}
			}
		default:
			if len(p.Values) == 0 {
				// An unresolvable claim fails closed: binding the '' it renders to
				// would emit a real predicate against the empty string — `col != ''`
				// or `col > ''` matches essentially every row, erasing the
				// restriction (#385). Matching the _in branch above, a filter scoped
				// to a claim the token doesn't carry matches no rows.
				clauses = append(clauses, "1 = 0")
				continue
			}
			clauses = append(clauses, fmt.Sprintf("%s %s ?", qcol, p.Op))
			params = append(params, p.Values[0])
		}
	}
	return clauses, params
}

// resolveFilters converts filter definitions with claim templates into SQL WHERE
// clauses. Retained as the predicates→SQL composition the query-path tests target.
func resolveFilters(filters map[string]Filter, claims map[string]any) ([]string, []any) {
	return predicatesToSQL(resolvePredicates(filters, claims))
}

// toStrings normalizes resolveInValues' []any (already canonical strings) to the
// []string a resolvedPredicate carries.
func toStrings(vals []any) []string {
	if len(vals) == 0 {
		return nil
	}
	out := make([]string, len(vals))
	for i, v := range vals {
		out[i] = fmt.Sprint(v)
	}
	return out
}

// resolveTemplate resolves {{ jwt.claim.path }} templates against JWT claims.
// If a claim path cannot be resolved — absent/null, a structured value (JSON
// object or array) rather than a scalar, or a numeric literal with no canonical
// form (see CanonicalScalar) — the placeholder is replaced with an empty string
// (never "<nil>") and ok is false, so filter callers can fail the predicate
// closed instead of binding a value the token never carried (#385). A template
// with no placeholders — including a literal "" — is always ok, and binds
// exactly as written: canonicalizing a numeric-spelled literal here would
// silently move read filters on String columns (`_neq: "1.0"` on a version
// column is a different predicate than `_neq: "1"`); the insert-check
// comparison instead accepts a literal's numeric reading at compare time
// (CanonicalNumericLiteral).
func resolveTemplate(tmpl string, claims map[string]any) (string, bool) {
	ok := true
	resolved := claimTemplateRe.ReplaceAllStringFunc(tmpl, func(match string) string {
		sub := claimTemplateRe.FindStringSubmatch(match)
		if len(sub) < 2 {
			// Unreachable: claimTemplateRe has exactly one capture group, so a match
			// always yields group 1. Kept as a fail-closed guard.
			ok = false
			return ""
		}
		s, bindable := CanonicalScalar(navigateClaims(claims, strings.Split(sub[1], ".")))
		if !bindable {
			ok = false
		}
		return s
	})
	return resolved, ok
}

// resolveInValues resolves a templated _in value into the set of bound values
// for a `col IN (?, …)` predicate. When the template is a single bare claim
// reference and that claim is a JSON array, every element becomes one value —
// the multi-tenant case, where a token's tenant_ids list scopes the predicate.
// A scalar claim (or any template with surrounding text) yields a single value,
// matching resolveTemplate. Every bound value routes through CanonicalScalar,
// so elements follow the same rule as the top-level claim: one structured,
// null, or canonical-form-less element fails the WHOLE set closed (nil) rather
// than binding a "map[…]"/"<nil>" rendering no row legitimately matches.
// Returns nil likewise when the claim itself is absent, a JSON object, or has
// no canonical form, so the caller can fail the predicate closed.
func resolveInValues(tmpl string, claims map[string]any) []any {
	if m := wholeClaimRe.FindStringSubmatch(strings.TrimSpace(tmpl)); m != nil {
		switch v := navigateClaims(claims, strings.Split(m[1], ".")).(type) {
		case []any:
			out := make([]any, 0, len(v))
			for _, e := range v {
				s, ok := CanonicalScalar(e)
				if !ok {
					return nil
				}
				out = append(out, s)
			}
			return out
		default:
			// Everything CanonicalScalar rejects — an absent/null claim, a
			// JSON-object claim (never a membership set), a number with no
			// canonical form — fails closed rather than bind as a one-element set.
			if s, ok := CanonicalScalar(v); ok {
				return []any{s}
			}
			return nil
		}
	}
	if v, ok := resolveTemplate(tmpl, claims); ok {
		return []any{v}
	}
	// A template with surrounding text whose claim is unresolvable also yields
	// nil (not a one-element set containing the partial literal), so it fails
	// closed like the bare-claim case above (#385).
	return nil
}

// maxCanonicalDigits bounds both a numeric literal's digit count and its
// exact decimal expansion. Big-integer parsing is superlinear in digit count
// and the ingest check path hands CanonicalScalar client-controlled literals
// (CWE-400), and a short exponent literal can hide a wide expansion ("1e-150"
// is six characters with a 152-character exact form). 100 digits is far past
// any real id — uint256 is 78.
const maxCanonicalDigits = 100

// CanonicalScalar renders a decoded JSON value as the canonical string the
// policy layer binds and compares, reporting ok=false for values with no such
// form: null, objects, and arrays. A structured value is never a sensible
// scalar comparison value — it usually means a dropped path segment
// ({{ jwt.app_metadata }} for {{ jwt.app_metadata.tenant_id }}), and binding
// fmt.Sprint's "map[…]"/"[…]" rendering would let _neq/_lt match essentially
// every row; the one legitimate structured shape, a bare-claim _in array, is
// unpacked by resolveInValues before its elements reach here. A json.Number
// (jwt.WithJSONNumber on claims, UseNumber on ingest payloads) binds in
// canonical decimal form, not the token's spelling: "1", "1.0", and "1e3" are
// one JSON value, and a numeric ClickHouse column rejects '1.0'/'1e3' as a
// per-query TYPE_MISMATCH error. The canonical form is exact at every width
// and precision — integer literals via big.Int, fractions and exponents via
// canonicalDecimal, never a float64 round-trip that could bind a value the
// token doesn't carry ("1e-400" fails closed rather than collapsing to "0").
// A literal, or an exact form, past maxCanonicalDigits likewise has no
// canonical form and fails closed (1e400, 1e-400). Claim resolution and the
// insert-check comparison's two sides (internal/api) all route through this
// one function, so what a read filter binds and what a write check accepts
// can't drift.
func CanonicalScalar(v any) (string, bool) {
	switch val := v.(type) {
	case nil, map[string]any, []any:
		return "", false
	case json.Number:
		// The digit bound guards the superlinear big.Int parse on the
		// client-controlled ingest path. It counts digits, not bytes — a sign,
		// point, or exponent marker doesn't feed the big parse — so "-1e99"
		// binds exactly like its written-out form, matching the "one JSON
		// value" contract below (only at the bound's very edge can the exact-form
		// gate's slack separate two spellings). canonicalDecimal re-checks its
		// own exact-form length, where a short literal can hide a wide expansion.
		digits := 0
		for i := 0; i < len(val); i++ {
			if val[i] >= '0' && val[i] <= '9' {
				digits++
			}
		}
		if digits > maxCanonicalDigits {
			return "", false
		}
		if i, ok := new(big.Int).SetString(val.String(), 10); ok {
			return i.String(), true
		}
		return canonicalDecimal(val.String())
	case float64:
		// Production claims arrive as json.Number (jwt.WithJSONNumber), but
		// Evaluate is an exported API and float64 is what a plain json.Unmarshal
		// hands any other caller. At or past 2^53 the decode has already
		// collapsed neighboring JSON integers onto one float — ANY digits
		// rendered for it could be another principal's ID — so refuse, in depth
		// (#381 review). Below that, render positionally: fmt.Sprint's exponent
		// form ("1e+06") is a spelling numeric ClickHouse columns reject.
		if math.Abs(val) >= 1<<53 {
			return "", false
		}
		return strconv.FormatFloat(val, 'f', -1, 64), true
	default:
		return fmt.Sprint(val), true
	}
}

// LiteralValue marks an insert-check required value the policy author wrote
// as a placeholder-free literal (Evaluate). A literal carries no JSON type —
// "1.0" means the number 1 to a numeric column and the three-character text
// to a String column — so the check comparison (internal/api) accepts its
// numeric reading as well as its spelling. The type is the gate: a
// claim-derived value is never wrapped, so a string-typed claim keeps strict
// canonical equality and can't gain a numeric reading it didn't have. Only
// CheckClauses carries this type; read filters bind plain strings.
type LiteralValue string

// CanonicalNumericLiteral renders a policy-authored literal that spells a
// JSON number in canonical decimal form ("1.0" → "1"), reporting ok=false for
// everything else. The json.Valid gate keeps this to spellings JSON itself
// can produce: big.Int would also take "+5" or "007", readings no decoded
// claim or payload value ever has. It canonicalizes nothing at resolve time —
// the literal still binds and auto-injects exactly as written; only the check
// comparison consults this second reading.
func CanonicalNumericLiteral(s string) (string, bool) {
	if !json.Valid([]byte(s)) {
		return "", false
	}
	return CanonicalScalar(json.Number(s))
}

// canonicalDecimal renders a non-integer JSON number literal (one carrying a
// fraction or exponent) as its exact canonical decimal string: "1.0" → "1",
// "2.50" → "2.5", "1e3" → "1000", "25e-4" → "0.0025", every digit preserved
// at any precision. ok=false for anything that is not a JSON number and for
// any literal whose exact decimal form would exceed maxCanonicalDigits.
// Exactness is the point: rounding through float64 would collapse "1e-400"
// to "0" and land wide decimals on their neighbors, either way binding a
// value the token doesn't carry.
func canonicalDecimal(lit string) (string, bool) {
	mant, expStr, hasExp := strings.Cut(lit, "e")
	if !hasExp {
		mant, expStr, hasExp = strings.Cut(lit, "E")
	}
	exp := 0
	if hasExp {
		var err error
		// Atoi rejects an empty or non-numeric exponent. The magnitude bound is
		// the allocation guard, not a redundancy: the exact-form length check at
		// the bottom runs only after strings.Repeat has already built the
		// string, so without this bound a 12-byte "1e1000000000" would allocate
		// a ~1 GiB expansion before being rejected.
		if exp, err = strconv.Atoi(expStr); err != nil || exp > 2*maxCanonicalDigits || exp < -2*maxCanonicalDigits {
			return "", false
		}
	}
	sign := ""
	if strings.HasPrefix(mant, "-") {
		sign, mant = "-", mant[1:]
	}
	intPart, fracPart, _ := strings.Cut(mant, ".")
	digits := intPart + fracPart
	if digits == "" {
		return "", false
	}
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return "", false
		}
	}
	// point is where the decimal point sits in digits once the exponent is
	// applied: digits[:point] to its left, digits[point:] to its right.
	point := len(intPart) + exp
	for len(digits) > 0 && digits[0] == '0' {
		digits = digits[1:]
		point--
	}
	for len(digits) > 0 && len(digits) > point && digits[len(digits)-1] == '0' {
		digits = digits[:len(digits)-1]
	}
	if digits == "" {
		// Every spelling of zero ("0.0", "-0e9") is the one value 0 —
		// signless, since a numeric column can't parse "-0".
		return "0", true
	}
	var out string
	switch {
	case point <= 0:
		out = "0." + strings.Repeat("0", -point) + digits
	case point >= len(digits):
		out = digits + strings.Repeat("0", point-len(digits))
	default:
		out = digits[:point] + "." + digits[point:]
	}
	// The exact form is what must stay small: "1e-150" is a six-character
	// literal with a 152-character expansion. The +2 slack exists for a "0."
	// prefix, though any form may spend it (a 102-digit integer expansion
	// passes). This runs after the Repeat above, so it bounds what callers
	// see — the exponent bound is what keeps the allocation itself small.
	if len(out) > maxCanonicalDigits+2 {
		return "", false
	}
	return sign + out, true
}

// navigateClaims traverses nested claim maps using dot-separated path parts.
func navigateClaims(claims map[string]any, parts []string) any {
	if len(parts) == 0 || claims == nil {
		return nil
	}
	val, ok := claims[parts[0]]
	if !ok {
		return nil
	}
	if len(parts) == 1 {
		return val
	}
	nested, ok := val.(map[string]any)
	if !ok {
		return nil
	}
	return navigateClaims(nested, parts[1:])
}

// IsColumnAllowed checks if a column is permitted by the resolved permissions.
// A nil receiver means no policy applies — all columns are allowed.
//
// This is the single source of truth for the per-column read decision. Every
// read path defers to it: the query builder checks every column a structured
// query references against it (internal/query.Build), and the stream path's
// filterColumns drops any event field it rejects. Keep it the one decision
// function so the surfaces can never drift apart.
func (rp *ResolvedPermissions) IsColumnAllowed(col string) bool {
	if rp == nil {
		return true
	}
	if !rp.Allowed {
		return false
	}
	// Precedence is most-restrictive-wins: the deny list is consulted before the
	// allow list, so a column in BOTH is denied. The order of the two loops is
	// cosmetic — the result is the conjunction "in allow AND not in deny" either
	// way; what would be unsafe is allow-WINS (returning true before consulting
	// deny), which we never do. See access-control.mdx "deny_columns always wins".
	//
	// "*" carries no special meaning here: in a query it is a literal column name,
	// decided by these same rules (and gated by schema membership in the builder,
	// so it only resolves when a real column is named "*"). The all-columns
	// wildcard is the caller's SelectAll, expanded by AllowedProjection — never a
	// bare "*" run through this function, which was the #223 footgun and is now
	// closed structurally.
	for _, d := range rp.DenyColumns {
		if d == col {
			return false
		}
	}
	// An empty allow list (or one containing the "*" wildcard token) means "all
	// columns": every non-denied column is permitted. A non-empty allow list
	// without "*" is an allowlist — only its members (never the denied) pass.
	if len(rp.AllowColumns) == 0 {
		return true
	}
	for _, a := range rp.AllowColumns {
		if a == "*" || a == col {
			return true
		}
	}
	return false
}

// AllowedProjection returns the subset of cols this role may read, preserving
// input order. It is the batch form of IsColumnAllowed and the single source of
// truth for expanding an unqualified "all columns" read (the SQL the builder
// would otherwise emit as SELECT *) into the concrete set a role is permitted to
// see — the projection counterpart to the stream path's filterColumns. A
// nil receiver (no policy) returns cols unchanged.
func (rp *ResolvedPermissions) AllowedProjection(cols []string) []string {
	if rp == nil {
		return cols
	}
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		if rp.IsColumnAllowed(c) {
			out = append(out, c)
		}
	}
	return out
}

// RestrictsColumns reports whether this role constrains which columns may be
// read — whether any column-level allow/deny rule applies. It is false only when
// the role can read every column (no deny list, and an allow list that is empty
// or a bare "*" wildcard); there a SELECT * exposes nothing the role isn't
// already entitled to, so the builder leaves it untouched. When true, the
// builder must expand SELECT * into AllowedProjection so denied columns never
// reach the result (#223). A nil receiver (no policy) is unrestricted; a denied
// role (`Allowed` false) restricts everything. The precedence here mirrors
// IsColumnAllowed exactly — including the `!Allowed` deny-all — so the "is this
// role restricted?" and "is this column allowed?" questions can never disagree.
func (rp *ResolvedPermissions) RestrictsColumns() bool {
	if rp == nil {
		return false
	}
	// A denied role can read no column, so it restricts everything. Without this
	// guard RestrictsColumns would return false ("unrestricted") for an
	// `Allowed:false` receiver while IsColumnAllowed denies all — and
	// resolveProjection's SelectAll branch trusts RestrictsColumns, so the
	// disagreement would emit a bare `SELECT *` over every column. Fail closed.
	if !rp.Allowed {
		return true
	}
	if len(rp.DenyColumns) > 0 {
		return true
	}
	if len(rp.AllowColumns) == 0 {
		return false
	}
	for _, a := range rp.AllowColumns {
		if a == "*" {
			return false
		}
	}
	return true
}

// IsAggregationAllowed checks if an aggregation function is permitted.
// A nil receiver means no policy applies — all aggregations are allowed.
func (rp *ResolvedPermissions) IsAggregationAllowed(fn string) bool {
	if rp == nil {
		return true
	}
	if !rp.Allowed {
		return false
	}
	// Normalize case once so both deny and allow checks are case-insensitive;
	// otherwise a cased aggregation (SUM) bypasses a lowercase deny entry (sum).
	fn = strings.ToLower(fn)
	// Check deny list.
	for _, d := range rp.DeniedAggregations {
		if strings.ToLower(d) == fn {
			return false
		}
	}
	// If allow list is empty, all non-denied are allowed.
	if len(rp.AllowedAggregations) == 0 {
		return true
	}
	for _, a := range rp.AllowedAggregations {
		if strings.ToLower(a) == fn {
			return true
		}
	}
	return false
}

// Validate checks that a policy is well-formed.
func Validate(p *Policy) error {
	if p == nil {
		return fmt.Errorf("policy is nil")
	}
	// Note: a default_role equal to the admin role is intentionally allowed (it
	// grants every roleless request full admin — a local/dev convenience). The
	// store warns loudly when such a policy is adopted (see
	// DefaultRoleGrantsAdmin); it is not a validation error.
	for tableName, tp := range p.Tables {
		for role, perms := range tp.Select {
			if err := validateRolePerms(tableName, "select", role, perms); err != nil {
				return err
			}
		}
		for role, perms := range tp.Insert {
			if err := validateRolePerms(tableName, "insert", role, perms); err != nil {
				return err
			}
		}
	}
	return nil
}

// malformedTemplate reports the first `{{ … }}` fragment of v that resolveTemplate
// would NOT recognize as a claim template — a path outside the [a-zA-Z0-9_.]
// grammar (a hyphen, a namespaced OIDC URL), a wrong or absent `jwt.` prefix,
// indexing, or an unterminated `{{`. resolveTemplate binds such a fragment as
// literal text, which is a fail-open on the filter path (`_neq`/`_lt` match ~every
// row) and silent row corruption on the check path, so validateRolePerms rejects
// it at write time — Store.Put, i.e. bootstrap-file adoption or an admin PUT; a
// policy already in KV is not re-validated when a node loads it (#461). A
// well-formed template — with or without surrounding literal
// text, e.g. "acct-{{ jwt.org_id }}" — returns ("", false).
func malformedTemplate(v string) (string, bool) {
	// Strip every well-formed template; a surviving "{{" is one the resolver
	// would leave as a literal instead of interpolating — except when stripping
	// itself splices one from the fragments around a well-formed template
	// ("{{{ jwt.a }}{" strips to "{{"), a rare over-rejection that errs
	// fail-closed at write time, never at query time.
	stripped := claimTemplateRe.ReplaceAllString(v, "")
	i := strings.Index(stripped, "{{")
	if i < 0 {
		return "", false
	}
	frag := stripped[i:]
	if end := strings.Index(frag, "}}"); end >= 0 {
		frag = frag[:end+2]
	}
	return frag, true
}

// rejectMalformedTemplates fails a policy at write time if any operator value in
// f carries a claim template the resolver would not interpolate (malformedTemplate).
func rejectMalformedTemplates(table, op, role, kind, col string, f Filter) error {
	for _, v := range []*string{f.Eq, f.Neq, f.Gt, f.Lt, f.In} {
		if v == nil {
			continue
		}
		if frag, bad := malformedTemplate(*v); bad {
			return fmt.Errorf("table %q, op %q, role %q: %s column %q references an unrecognized claim template %q — claim paths may contain only letters, digits, '_' and '.'; flatten hyphenated or namespaced (OIDC URL) claims at the identity provider", table, op, role, kind, col, frag)
		}
	}
	return nil
}

func validateRolePerms(table, op, role string, perms RolePermissions) error {
	if strings.TrimSpace(role) == "" {
		return fmt.Errorf("table %q, op %q: empty role name is not allowed", table, op)
	}
	if perms.MaxRows < 0 {
		return fmt.Errorf("table %q, op %q, role %q: max_rows must be non-negative", table, op, role)
	}
	if perms.MaxExecutionTime < 0 {
		return fmt.Errorf("table %q, op %q, role %q: max_execution_time must be non-negative", table, op, role)
	}
	if perms.MaxRowsToRead < 0 {
		return fmt.Errorf("table %q, op %q, role %q: max_rows_to_read must be non-negative", table, op, role)
	}
	if perms.MaxMemoryUsage < 0 {
		return fmt.Errorf("table %q, op %q, role %q: max_memory_usage must be non-negative", table, op, role)
	}
	// Filter and check column names are interpolated into SQL (backtick-quoted) at
	// query time, so a '?' in one would shift clickhouse-go's positional value
	// binding. Refuse such a policy at write time, mirroring the query builder's
	// chsql.BindUnsafe guard on caller-supplied columns.
	for col, f := range perms.Filter {
		if chsql.BindUnsafe(col) {
			return fmt.Errorf("table %q, op %q, role %q: filter column %q contains '?' (unsupported)", table, op, role, col)
		}
		// The filter path enforces every operator (_eq/_neq/_gt/_lt/_in), so no
		// operator is rejected here. A malformed claim template IS rejected: the
		// resolver would bind it as a literal, a fail-open for _neq/_lt (#385/#457).
		if err := rejectMalformedTemplates(table, op, role, "filter", col, f); err != nil {
			return err
		}
	}
	for col, f := range perms.Check {
		if chsql.BindUnsafe(col) {
			return fmt.Errorf("table %q, op %q, role %q: check column %q contains '?' (unsupported)", table, op, role, col)
		}
		// The check/insert path honors only _eq (a required value) and _in (a
		// required set); the comparison operators have no insert-time semantics and
		// no auto-inject, so reject them loudly at write time (#224).
		if f.Neq != nil || f.Gt != nil || f.Lt != nil {
			return fmt.Errorf("table %q, op %q, role %q: check column %q uses _neq/_gt/_lt, which check does not honor (use _eq or _in)", table, op, role, col)
		}
		// _eq and _in are both honored, but they carry different required-value
		// semantics (a single value vs. set membership) and Evaluate resolves only
		// one — _eq wins, silently dropping _in. Reject the ambiguous pair at write
		// time rather than enforce an arbitrary branch (the same accept-but-ignore
		// gap #224 closes).
		if f.Eq != nil && f.In != nil {
			return fmt.Errorf("table %q, op %q, role %q: check column %q sets both _eq and _in; use exactly one", table, op, role, col)
		}
		// A malformed claim template on the check path is rejected too: the resolver
		// would bind the literal `{{…}}` text as the required value and stamp it into
		// every auto-injected row (#385/#457; the required-value question is #463).
		if err := rejectMalformedTemplates(table, op, role, "check", col, f); err != nil {
			return err
		}
	}
	return nil
}

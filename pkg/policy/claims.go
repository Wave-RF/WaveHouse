package policy

import (
	"regexp"
	"strings"
)

// ClaimTemplatePattern is the `{{ jwt.<path> }}` grammar as a regexp source,
// group 1 the claim path: the one definition pipe SQL and policy values share,
// so a reference binds the same way on every surface. A path is letters,
// digits, `_` and `.`; anything else is not a template (see Validate's
// malformed-template rejection, and NamedQuery.CheckTemplate in pkg/pipes).
const ClaimTemplatePattern = `\{\{\s*jwt\.([a-zA-Z0-9_.]+)\s*\}\}`

// claimTemplateRe matches {{ jwt.claim.path }} templates.
var claimTemplateRe = regexp.MustCompile(ClaimTemplatePattern)

// wholeClaimRe matches a value that is EXACTLY one {{ jwt.path }} reference with
// no surrounding text (anchored, unlike claimTemplateRe). It lets the _in
// resolver pull an array-valued claim through whole — col IN (its elements) —
// rather than stringifying it as resolveTemplate would.
var wholeClaimRe = regexp.MustCompile(`^` + ClaimTemplatePattern + `$`)

// ResolveClaim resolves one claim path against a verified token's claims for
// binding: a scalar claim is one value in canonical form (CanonicalScalar),
// and an array claim is the canonical form of each element, reported as a
// set (set is true), the multi-tenant shape where a token's tenant_ids list
// scopes a predicate. ok is false when nothing can bind, so the caller fails
// closed rather than bind a value the token never carried (#385): the claim
// is absent or null, a JSON object, an empty array (no set to be a member
// of), an element that is not a scalar, or a number with no canonical form.
// This is the resolution a policy `_in` value gets; a pipe's `{{ jwt.<path> }}`
// reference binds through it too, so the two surfaces cannot drift.
func ResolveClaim(claims map[string]any, path string) (values []string, set, ok bool) {
	switch v := navigateClaims(claims, strings.Split(path, ".")).(type) {
	case []any:
		if len(v) == 0 {
			return nil, true, false
		}
		values = make([]string, 0, len(v))
		for _, e := range v {
			s, ok := CanonicalScalar(e)
			if !ok {
				return nil, true, false
			}
			values = append(values, s)
		}
		return values, true, true
	default:
		s, ok := CanonicalScalar(v)
		if !ok {
			return nil, false, false
		}
		return []string{s}, false, true
	}
}

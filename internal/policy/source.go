package policy

// Source yields the policy a request is evaluated against. It is read per
// call — never cached by the consumer — so a settings-directory reload
// applies to the very next request. A nil *Policy from a wired Source is a
// deliberate lockout (Evaluate denies everything but the operator key); a
// nil Source means policy filtering is not wired at all, which only tests
// do. In production the Source is settings.Store.Policy.
type Source func() *Policy

// Static returns a Source fixed to p. Intended for tests.
func Static(p *Policy) Source { return func() *Policy { return p } }

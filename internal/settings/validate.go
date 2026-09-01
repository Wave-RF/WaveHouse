package settings

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/Wave-RF/WaveHouse/internal/pipes"
	"github.com/Wave-RF/WaveHouse/internal/policy"
)

// Validate reads, decodes, and checks a settings directory in one pass and
// returns every finding it can discover — an operator fixing a hand-edited
// directory wants the whole list, not a fix-rerun-fix loop. No side effects:
// no network, no ClickHouse, nothing written. The Document is returned only
// when no finding is an error (warnings alone leave it usable), so what a
// node adopts is byte-for-byte what was validated — this is the single
// checking path, and every consumer (the `wavehouse validate` CLI, boot, a
// live reload) goes through it. Callers translate the result per their own
// contract: the CLI exits non-zero, boot refuses to start, and a live reload
// keeps serving the previous good document.
func Validate(dir string) (*Document, []Finding) {
	v := &validator{}

	files, ok := v.checkDir(dir)
	if !ok {
		return nil, v.findings
	}

	doc := &Document{}
	roles, rolesOK := v.parseRoles(files[FileRoles])
	doc.Roles = roles
	doc.Policy = v.parsePolicies(files[FilePolicies])
	doc.Pipes = v.parsePipes(files[FilePipes])
	doc.Config = v.parseConfig(files[FileConfig])

	// Cross-file referential integrity needs a trustworthy registry; when
	// roles.json itself failed, its own finding already says why every
	// reference check would be noise.
	if rolesOK {
		v.checkRoleRefs(roles, doc.Policy, doc.Pipes)
	}

	if HasErrors(v.findings) {
		return nil, v.findings
	}
	return doc, v.findings
}

type validator struct {
	findings []Finding
}

func (v *validator) errorf(file, path, format string, args ...any) {
	v.findings = append(v.findings, Finding{Severity: SeverityError, File: file, Path: path, Message: fmt.Sprintf(format, args...)})
}

func (v *validator) warnf(file, path, format string, args ...any) {
	v.findings = append(v.findings, Finding{Severity: SeverityWarning, File: file, Path: path, Message: fmt.Sprintf(format, args...)})
}

// checkDir verifies the directory itself and reads each expected file's bytes.
// A missing file is an error — an empty deployment writes {} in all four
// files, so absence always means deletion or a wrong path, never "defaults".
// Any other entry — file or subdirectory, JSON or not — is an error: a typoed
// polices.json silently ignored would revert the author's intent, the exact
// trap strict decoding closes for keys. The one exception is dot-prefixed
// entries (see the loop body).
func (v *validator) checkDir(dir string) (map[string][]byte, bool) {
	info, err := os.Stat(dir)
	switch {
	case os.IsNotExist(err):
		v.errorf("", "", "settings directory %q does not exist", dir)
		return nil, false
	case err != nil:
		v.errorf("", "", "settings directory %q: %v", dir, err)
		return nil, false
	case !info.IsDir():
		v.errorf("", "", "settings path %q is not a directory", dir)
		return nil, false
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		v.errorf("", "", "read settings directory %q: %v", dir, err)
		return nil, false
	}

	expected := Files()
	files := map[string][]byte{}
	for _, e := range entries {
		name := e.Name()
		// Dot-prefixed entries are the machinery of atomic writers and
		// editors — Kubernetes ConfigMap mounts publish through `..data`
		// symlink directories, vim keeps `.foo.swp` — so they are the one
		// thing tolerated besides the four files. Everything else unexpected
		// is an error: a stray entry in a control-plane directory is a typo
		// or a leak, never decoration.
		if strings.HasPrefix(name, ".") {
			continue
		}
		if !slices.Contains(expected, name) {
			kind := "file"
			if e.IsDir() {
				kind = "directory"
			}
			v.errorf(name, "", "unexpected %s — the settings directory holds only %s", kind, strings.Join(expected, ", "))
			continue
		}
		if e.IsDir() {
			// A directory squatting on a settings filename would otherwise
			// surface as "missing" — name the real problem. The nil entry
			// marks it handled so exactly one finding is emitted.
			v.errorf(name, "", "is a directory — expected a JSON file")
			files[name] = nil
			continue
		}
		// Clean is redundant after Join but satisfies gosec G304: dir can
		// arrive from CLI args/env (taint sources), unlike config-struct paths.
		path := filepath.Clean(filepath.Join(dir, name))
		// Stat gate before the read: os.ReadFile on a FIFO blocks until a
		// writer connects, which would hang validate (and, later, a reload)
		// with no finding and no exit. Stat follows symlinks, so the
		// symlink-to-regular-file layout Kubernetes ConfigMap mounts use
		// still passes; anything else non-regular is rejected.
		info, err := os.Stat(path)
		switch {
		case err != nil:
			v.errorf(name, "", "stat: %v", err)
			files[name] = nil // handled — don't also report it as missing
			continue
		case !info.Mode().IsRegular():
			v.errorf(name, "", "not a regular file (mode %s) — expected a JSON file", info.Mode().Type())
			files[name] = nil
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			v.errorf(name, "", "read: %v", err)
			files[name] = nil
			continue
		}
		files[name] = data
	}
	for _, name := range expected {
		if _, present := files[name]; !present {
			v.errorf(name, "", "missing — every settings file must exist (an empty document is {})")
		}
	}
	return files, true
}

// parseFile runs the shared syntax gate — non-empty, duplicate-key scan,
// strict decode — and reports whether target was populated.
func (v *validator) parseFile(name string, data []byte, target any) bool {
	if data == nil {
		return false // missing/unreadable file, already reported
	}
	if bytes.HasPrefix(data, []byte("\xef\xbb\xbf")) {
		// Notepad and friends prepend this; without the check it surfaces as a
		// cryptic `invalid character 'ï'` decode error.
		v.errorf(name, "", "file begins with a UTF-8 byte order mark — save without BOM")
		return false
	}
	switch strings.TrimSpace(string(data)) {
	case "":
		// An all-whitespace file is what a truncated save looks like; it must
		// never be read as an empty document.
		v.errorf(name, "", "file is empty — not valid JSON; an empty document is {}")
		return false
	case "null":
		// The one well-formed document decodeStrict can't catch: a top-level
		// null decodes into the zero value without error, silently reading as
		// "no settings". (Other non-object roots already fail the decode.)
		v.errorf(name, "", "document is null — an empty document is {}")
		return false
	}
	v.findings = append(v.findings, dupKeyFindings(name, data)...)
	if err := decodeStrict(data, target); err != nil {
		v.errorf(name, "", "%v", err)
		return false
	}
	return true
}

func (v *validator) parseRoles(data []byte) ([]string, bool) {
	var f RolesFile
	if !v.parseFile(FileRoles, data, &f) {
		return nil, false
	}
	seen := map[string]bool{}
	for i, role := range f.Roles {
		path := fmt.Sprintf("roles[%d]", i)
		switch {
		case role == "":
			v.errorf(FileRoles, path, "role name must not be empty")
		case strings.TrimSpace(role) != role:
			v.errorf(FileRoles, path, "role name %q has surrounding whitespace", role)
		case seen[role]:
			v.errorf(FileRoles, path, "duplicate role %q", role)
		}
		seen[role] = true
	}
	return f.Roles, true
}

// parsePolicies returns nil for an empty document: no policy, fail closed —
// the same semantics as a deleted policy in the store. That state is legal
// but flagged with a warning: the total lockout should announce itself at
// validation time, not be discovered one 403 at a time.
func (v *validator) parsePolicies(data []byte) *policy.Policy {
	// Run the layout check first: a pre-v2 document decodes into either a
	// baffling "unknown field" error or, for an empty operation block, a silent
	// no-op grant. Neither says "your file is the old shape".
	if v.reportLegacyPolicyLayout(data) {
		return nil
	}
	var p policy.Policy
	if !v.parseFile(FilePolicies, data, &p) {
		return nil
	}
	if p.DefaultRole == "" && p.AdminRole == "" && len(p.Tables) == 0 {
		v.warnf(FilePolicies, "", "empty document — no policy; every request will be denied (fail closed)")
		return nil
	}
	if err := policy.Validate(&p); err != nil {
		v.errorf(FilePolicies, "", "%v", err)
	}
	return &p
}

// selectPermFields and insertPermFields are the JSON field names of
// policy.SelectPermissions and policy.InsertPermissions. They exist only to tell
// a v2 operation block apart from a pre-v2 map of role grants; keep them in sync
// with those structs (a stale entry costs a missed diagnostic, never a wrong
// decode — strict decoding still owns correctness).
var (
	selectPermFields = map[string]bool{
		"allow_columns": true, "deny_columns": true, "filter": true,
		"allowed_aggregations": true, "denied_aggregations": true,
		"max_rows": true, "max_execution_time": true,
		"max_rows_to_read": true, "max_memory_usage": true,
	}
	insertPermFields = map[string]bool{
		"allow_columns": true, "deny_columns": true, "check": true,
	}
)

// reportLegacyPolicyLayout detects the pre-v2 operation-first layout —
// tables.<table>.select.<role> — and reports it as one clear finding, returning
// true when it did. In v2 the nesting is tables.<table>.<role>.select, so an old
// document's "select" block is a map of role names to grant objects rather than
// the permission object v2 expects.
//
// The evidence required is deliberately strong: a non-empty object under
// "select"/"insert" whose every value is itself an object and none of whose keys
// is a v2 permission field. The one shape that still reads as legacy is a table
// granting a role literally named "select" or "insert" — rename such a role.
// A malformed document is left alone; the JSON parse error is the better message.
func (v *validator) reportLegacyPolicyLayout(data []byte) bool {
	var doc struct {
		Tables map[string]map[string]json.RawMessage `json:"tables"`
	}
	if json.Unmarshal(data, &doc) != nil {
		return false
	}
	// Sorted so a document with two legacy tables names the same one every run —
	// map order would make the finding nondeterministic. Matches parseConfig's
	// convention further down this file.
	for _, table := range slices.Sorted(maps.Keys(doc.Tables)) {
		tp := doc.Tables[table]
		for _, op := range []string{"select", "insert"} {
			raw, ok := tp[op]
			if !ok {
				continue
			}
			fields := selectPermFields
			if op == "insert" {
				fields = insertPermFields
			}
			if !looksLikeRoleMap(raw, fields) {
				continue
			}
			v.errorf(FilePolicies, fmt.Sprintf("tables.%s.%s", table, op),
				"pre-v2 operation-first layout (tables.%s.%s.<role>); v2 is role-first (tables.%s.<role>.%s) — see wavehouse.dev/access-control#migrating-from-the-operation-first-layout",
				table, op, table, op)
			return true
		}
	}
	return false
}

// looksLikeRoleMap reports whether raw is a non-empty JSON object that reads as
// a map of role name → grant object rather than as an operation's permission
// object (whose keys would be in fields).
func looksLikeRoleMap(raw json.RawMessage, fields map[string]bool) bool {
	var obj map[string]json.RawMessage
	if json.Unmarshal(raw, &obj) != nil || len(obj) == 0 {
		return false
	}
	for k, val := range obj {
		if fields[k] {
			return false
		}
		if len(bytes.TrimSpace(val)) == 0 || bytes.TrimSpace(val)[0] != '{' {
			return false
		}
	}
	return true
}

// validParamType reports whether t is a documented ParamDef.Type ("" =
// unspecified). A switch rather than a package-level slice so the set is
// genuinely constant.
func validParamType(t string) bool {
	switch t {
	case "", "string", "number", "boolean", "array":
		return true
	}
	return false
}

func (v *validator) parsePipes(data []byte) []pipes.NamedQuery {
	var f PipesFile
	if !v.parseFile(FilePipes, data, &f) {
		return nil
	}
	seen := map[string]bool{}
	for i, q := range f.Pipes {
		path := fmt.Sprintf("pipes[%d]", i)
		switch {
		case q.Name == "":
			v.errorf(FilePipes, path, "pipe name must not be empty")
		case seen[q.Name]:
			v.errorf(FilePipes, path, "duplicate pipe %q", q.Name)
		}
		seen[q.Name] = true
		if strings.TrimSpace(q.SQL) == "" {
			v.errorf(FilePipes, path+".sql", "pipe SQL must not be empty")
		}
		seenParams := map[string]bool{}
		for j, param := range q.Parameters {
			ppath := fmt.Sprintf("%s.parameters[%d]", path, j)
			switch {
			case param.Name == "":
				v.errorf(FilePipes, ppath, "parameter name must not be empty")
			case seenParams[param.Name]:
				v.errorf(FilePipes, ppath, "duplicate parameter %q", param.Name)
			}
			seenParams[param.Name] = true
			if !validParamType(param.Type) {
				v.errorf(FilePipes, ppath+".type", "unknown type %q — expected one of string, number, boolean, array", param.Type)
			}
			if param.Required && param.Default != nil {
				v.warnf(FilePipes, ppath, "default on a required parameter is never used")
			}
		}
	}
	return f.Pipes
}

// checkIDField rejects an id_field value that could never match a JSON key:
// empty or whitespace-only (the author wrote a value that names nothing) and
// surrounding whitespace (an exact-match lookup would silently miss every
// row). nil is the caller's concern — required at the top level, inherit in
// a table override. Interior whitespace stays legal: "click id" is a valid
// JSON key.
func (v *validator) checkIDField(path string, val *string) {
	if val == nil {
		return
	}
	switch {
	case strings.TrimSpace(*val) == "":
		v.errorf(FileConfig, path, "must not be empty")
	case strings.TrimSpace(*val) != *val:
		v.errorf(FileConfig, path, "id_field %q has surrounding whitespace", *val)
	}
}

// checkTableName rejects a per-table override key that could never match a
// table: empty, or carrying surrounding whitespace. Shared by the dedupe and
// dlq override maps.
func (v *validator) checkTableName(mapPath, table string) {
	switch {
	case table == "":
		v.errorf(FileConfig, mapPath, "table name must not be empty")
	case strings.TrimSpace(table) != table:
		v.errorf(FileConfig, mapPath+"."+table, "table name %q has surrounding whitespace", table)
	}
}

// checkClickHouse validates the connection wiring: every field required, the
// native address a host:port, the HTTP port in range, the scheme http or
// https, and the timeout positive. Reachability is deliberately not checked
// — Validate is pure, and adoption is unconditional; an unreachable address
// surfaces through schema discovery and /readyz like any other outage.
func (v *validator) checkClickHouse(ch *ClickHouseConfig) {
	if ch.Addr == nil {
		v.required("clickhouse.addr")
	} else if host, port, err := net.SplitHostPort(*ch.Addr); err != nil || host == "" || port == "" {
		v.errorf(FileConfig, "clickhouse.addr", "must be host:port, got %q", *ch.Addr)
	}
	if ch.HTTPPort == nil {
		v.required("clickhouse.http_port")
	} else if *ch.HTTPPort < 1 || *ch.HTTPPort > 65535 {
		v.errorf(FileConfig, "clickhouse.http_port", "must be in 1-65535, got %d", *ch.HTTPPort)
	}
	if ch.HTTPScheme == nil {
		v.required("clickhouse.http_scheme")
	} else if *ch.HTTPScheme != "http" && *ch.HTTPScheme != "https" {
		v.errorf(FileConfig, "clickhouse.http_scheme", "must be \"http\" or \"https\", got %q", *ch.HTTPScheme)
	}
	for path, val := range map[string]*string{"clickhouse.database": ch.Database, "clickhouse.username": ch.Username} {
		if val == nil {
			v.required(path)
		} else if strings.TrimSpace(*val) == "" {
			v.errorf(FileConfig, path, "must not be empty")
		}
	}
	if ch.QueryTimeout == nil {
		v.required("clickhouse.query_timeout")
	} else if *ch.QueryTimeout < 1 {
		v.errorf(FileConfig, "clickhouse.query_timeout", "must be >= 1 second, got %d", *ch.QueryTimeout)
	}
}

// required reports a missing key. Every top-level tunable is required so the
// adopted snapshot never depends on a value the files don't state.
func (v *validator) required(path string) {
	v.errorf(FileConfig, path, "required — run `wavehouse bootstrap` for a complete starter config.json")
}

func (v *validator) parseConfig(data []byte) TenantConfig {
	var c TenantConfig
	if !v.parseFile(FileConfig, data, &c) {
		// Not swallowed: parseFile already recorded the error finding. The
		// zero value just lets the one-pass sweep continue through the other
		// files; the document is discarded whenever any error exists.
		return TenantConfig{}
	}
	if ch := c.ClickHouse; ch == nil {
		v.required("clickhouse")
	} else {
		v.checkClickHouse(ch)
	}
	if a := c.Auth; a == nil {
		v.required("auth")
	} else {
		if a.JWKSURL == nil {
			v.required("auth.jwks_url")
		} else if *a.JWKSURL != "" {
			u, err := url.Parse(*a.JWKSURL)
			switch {
			case err != nil:
				v.errorf(FileConfig, "auth.jwks_url", "not a valid URL: %v", err)
			case (u.Scheme != "http" && u.Scheme != "https") || u.Host == "":
				v.errorf(FileConfig, "auth.jwks_url", "must be an absolute http(s) URL, got %q", *a.JWKSURL)
			}
		}
		if a.RoleClaim == nil {
			v.required("auth.role_claim")
		} else if strings.TrimSpace(*a.RoleClaim) == "" || strings.TrimSpace(*a.RoleClaim) != *a.RoleClaim {
			v.errorf(FileConfig, "auth.role_claim", "must be a non-empty claim path with no surrounding whitespace, got %q", *a.RoleClaim)
		}
	}
	if d := c.Dedupe; d == nil {
		v.required("dedupe")
	} else {
		if d.Enabled == nil {
			v.required("dedupe.enabled")
		}
		if d.IDField == nil {
			v.required("dedupe.id_field")
		}
		if d.RequireID == nil {
			v.required("dedupe.require_id")
		}
		v.checkIDField("dedupe.id_field", d.IDField)
		// Sorted iteration keeps finding order deterministic across runs.
		for _, table := range slices.Sorted(maps.Keys(d.Tables)) {
			td := d.Tables[table]
			path := "dedupe.tables." + table
			v.checkTableName("dedupe.tables", table)
			v.checkIDField(path+".id_field", td.IDField)
			if td.IDField == nil && td.RequireID == nil {
				v.warnf(FileConfig, path, "override sets nothing — remove it, or set id_field or require_id")
			}
		}
	}
	if d := c.DLQ; d == nil {
		v.required("dlq")
	} else {
		if d.Enabled == nil {
			v.required("dlq.enabled")
		}
		for _, table := range slices.Sorted(maps.Keys(d.Tables)) {
			path := "dlq.tables." + table
			v.checkTableName("dlq.tables", table)
			if d.Tables[table].Enabled == nil {
				v.warnf(FileConfig, path, "override sets nothing — remove it, or set enabled")
			}
		}
	}
	if q := c.Query; q == nil {
		v.required("query")
	} else {
		if q.DefaultMaxRows == nil {
			v.required("query.default_max_rows")
		} else if *q.DefaultMaxRows < 1 {
			v.errorf(FileConfig, "query.default_max_rows", "must be >= 1, got %d", *q.DefaultMaxRows)
		}
		if q.TimestampBucketSeconds == nil {
			v.required("query.timestamp_bucket_seconds")
		} else if *q.TimestampBucketSeconds < 0 {
			v.errorf(FileConfig, "query.timestamp_bucket_seconds", "must be >= 0 (0 disables bucketing), got %d", *q.TimestampBucketSeconds)
		}
	}
	if s := c.Schema; s == nil {
		v.required("schema")
	} else if s.RefreshInterval == nil {
		v.required("schema.refresh_interval")
	} else if *s.RefreshInterval < 1 {
		v.errorf(FileConfig, "schema.refresh_interval", "must be >= 1 second, got %d", *s.RefreshInterval)
	}
	if s := c.Stream; s == nil {
		v.required("stream")
	} else {
		if s.KeepaliveInterval == nil {
			v.required("stream.keepalive_interval")
		} else if *s.KeepaliveInterval < 1 {
			v.errorf(FileConfig, "stream.keepalive_interval", "must be >= 1 second, got %d", *s.KeepaliveInterval)
		}
		if s.KeepaliveBuckets == nil {
			v.required("stream.keepalive_buckets")
		} else if *s.KeepaliveBuckets < 1 {
			v.errorf(FileConfig, "stream.keepalive_buckets", "must be >= 1, got %d", *s.KeepaliveBuckets)
		}
		if s.GapWindowMinutes == nil {
			v.required("stream.gap_window_minutes")
		} else if *s.GapWindowMinutes < 0 {
			v.errorf(FileConfig, "stream.gap_window_minutes", "must be >= 0, got %d", *s.GapWindowMinutes)
		}
	}

	if m := c.MQ; m == nil {
		v.required("mq")
	} else if m.MaxBytesGB == nil {
		v.required("mq.max_bytes_gb")
	} else if *m.MaxBytesGB < 1 {
		v.errorf(FileConfig, "mq.max_bytes_gb", "must be >= 1 GB, got %d", *m.MaxBytesGB)
	}

	switch co := c.CORS; {
	case co == nil:
		v.required("cors")
	case co.AllowedOrigins == nil:
		v.required("cors.allowed_origins")
	default:
		for i, origin := range co.AllowedOrigins {
			if strings.TrimSpace(origin) == "" {
				v.errorf(FileConfig, fmt.Sprintf("cors.allowed_origins[%d]", i), "origin must not be empty")
			}
		}
	}
	return c
}

// checkRoleRefs enforces referential integrity against the roles.json
// registry: every role a policy or pipe references must be declared. The
// effective admin role is the exception — it is compiled-in (policy.AdminRole
// defaults it to "admin") and an unconditional bypass, so a grant scoping it
// is dead config worth a warning, not a registry lookup.
func (v *validator) checkRoleRefs(roles []string, p *policy.Policy, queries []pipes.NamedQuery) {
	declared := map[string]bool{}
	for _, r := range roles {
		declared[r] = true
	}
	// AdminRole(nil) is "" — with no policy nothing matches the admin branch.
	admin := policy.AdminRole(p)

	if p != nil {
		if p.DefaultRole != "" && !declared[p.DefaultRole] {
			v.errorf(FilePolicies, "default_role", "role %q is not declared in %s", p.DefaultRole, FileRoles)
		}
		if p.AdminRole != "" && !declared[p.AdminRole] {
			v.errorf(FilePolicies, "admin_role", "role %q is not declared in %s", p.AdminRole, FileRoles)
		}
		if policy.DefaultRoleGrantsAdmin(p) {
			v.warnf(FilePolicies, "default_role", "default_role equals the admin role — every roleless request gets full admin; avoid outside local dev")
		}
		for table, tp := range p.Tables {
			for role, grant := range tp {
				path := fmt.Sprintf("tables.%s.%s", table, role)
				if role == "" {
					v.errorf(FilePolicies, path, "grant role must not be empty — an empty role matches no request")
					continue
				}
				if role == admin {
					v.warnf(FilePolicies, path, "admin is an unconditional bypass — this grant has no effect")
					continue
				}
				if !declared[role] {
					v.errorf(FilePolicies, path, "role %q is not declared in %s", role, FileRoles)
					continue
				}
				// A grant with neither operation authorizes nothing. Legal, but
				// almost always a half-finished edit — and the shape a pre-v2
				// `"select": {}` block silently decodes into, which the layout
				// detector cannot flag. Say so rather than let it read as a grant.
				if grant.Select == nil && grant.Insert == nil {
					v.warnf(FilePolicies, path, "grant sets neither select nor insert — this role gets no access to the table")
				}
			}
		}
	}

	for i, q := range queries {
		for j, role := range q.AllowedRoles {
			path := fmt.Sprintf("pipes[%d].allowed_roles[%d]", i, j)
			if role == "" {
				v.errorf(FilePipes, path, "role must not be empty — an empty allowlist entry authorizes nobody")
				continue
			}
			// role is non-empty here, so this can't false-match the ""
			// AdminRole(nil) returns when there is no policy.
			if role == admin {
				v.warnf(FilePipes, path, "the admin role can always execute pipes — listing it is redundant")
				continue
			}
			if !declared[role] {
				v.errorf(FilePipes, path, "role %q is not declared in %s", role, FileRoles)
			}
		}
	}
}

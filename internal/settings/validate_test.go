package settings

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeDir materializes a settings directory from name → content.
func writeDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
	}
	return dir
}

// validFiles is a complete, correct directory; tests override single entries.
func validFiles() map[string]string {
	return map[string]string{
		FileRoles:    `{"roles": ["public", "analyst", "admin"]}`,
		FilePolicies: `{"default_role": "public", "tables": {"clicks": {"select": {"analyst": {"max_rows": 100}}}}}`,
		FilePipes:    `{"pipes": [{"name": "top_clicks", "sql": "SELECT 1", "allowed_roles": ["analyst"], "parameters": [{"name": "limit", "type": "number"}]}]}`,
		FileConfig:   configJSON(`{"dedupe": {"tables": {"clicks": {"id_field": "click_id"}}}}`),
	}
}

// configJSON returns the seed config.json with patch merged over it, one
// level deep (a patched block's keys replace the seed's, the rest of the
// block is kept). Every key is required, so tests that care about one key
// build a complete document from the seed rather than repeating all of them.
func configJSON(patch string) string {
	seed, err := Seed()
	if err != nil {
		panic(err)
	}
	var base, over map[string]map[string]json.RawMessage
	if err := json.Unmarshal(seed[FileConfig], &base); err != nil {
		panic(err)
	}
	if err := json.Unmarshal([]byte(patch), &over); err != nil {
		panic(err)
	}
	for block, keys := range over {
		if base[block] == nil {
			base[block] = map[string]json.RawMessage{}
		}
		for k, v := range keys {
			base[block][k] = v
		}
	}
	out, err := json.Marshal(base)
	if err != nil {
		panic(err)
	}
	return string(out)
}

func findingStrings(findings []Finding) string {
	parts := make([]string, len(findings))
	for i, f := range findings {
		parts[i] = f.String()
	}
	return strings.Join(parts, "\n")
}

func TestValidate_ValidDirectory(t *testing.T) {
	t.Parallel()
	doc, findings := Validate(writeDir(t, validFiles()))

	require.Empty(t, findings, "a fully valid directory must produce no findings")
	require.NotNil(t, doc)
	assert.Equal(t, []string{"public", "analyst", "admin"}, doc.Roles)
	require.NotNil(t, doc.Policy)
	assert.Equal(t, "public", doc.Policy.DefaultRole)
	require.Len(t, doc.Pipes, 1)
	assert.Equal(t, "top_clicks", doc.Pipes[0].Name)
	require.NotNil(t, doc.Config.Dedupe)
	assert.Equal(t, "event_id", *doc.Config.Dedupe.IDField)
	require.Contains(t, doc.Config.Dedupe.Tables, "clicks")
	assert.Equal(t, "click_id", *doc.Config.Dedupe.Tables["clicks"].IDField)
	assert.Nil(t, doc.Config.Dedupe.Tables["clicks"].RequireID, "unset override field stays nil (inherit)")
	require.NotNil(t, doc.Config.Schema)
	assert.Equal(t, 60, *doc.Config.Schema.RefreshInterval)
}

func TestValidate_EmptyDocuments(t *testing.T) {
	t.Parallel()
	doc, findings := Validate(writeDir(t, map[string]string{
		FileRoles: `{}`, FilePolicies: `{}`, FilePipes: `{}`, FileConfig: configJSON(`{}`),
	}))

	// Valid — but not silent: the one finding is the no-policy lockout warning,
	// so the total deny announces itself at validation time, not per-403.
	require.NotNil(t, doc)
	assert.False(t, HasErrors(findings))
	require.Len(t, findings, 1)
	assert.Contains(t, findingStrings(findings), "every request will be denied")
	assert.Nil(t, doc.Policy, "an empty policies.json means no policy (fail closed)")
	assert.Empty(t, doc.Roles)
	assert.Empty(t, doc.Pipes)
}

func TestValidate_DirectoryProblems(t *testing.T) {
	t.Parallel()

	t.Run("missing directory", func(t *testing.T) {
		t.Parallel()
		doc, findings := Validate(filepath.Join(t.TempDir(), "nope"))
		assert.Nil(t, doc)
		require.True(t, HasErrors(findings))
		assert.Contains(t, findingStrings(findings), "does not exist")
	})

	t.Run("path is a file", func(t *testing.T) {
		t.Parallel()
		f := filepath.Join(t.TempDir(), "file")
		require.NoError(t, os.WriteFile(f, []byte("x"), 0o600))
		doc, findings := Validate(f)
		assert.Nil(t, doc)
		assert.Contains(t, findingStrings(findings), "not a directory")
	})

	t.Run("missing file", func(t *testing.T) {
		t.Parallel()
		files := validFiles()
		delete(files, FileConfig)
		doc, findings := Validate(writeDir(t, files))
		assert.Nil(t, doc)
		assert.Contains(t, findingStrings(findings), "config.json: missing")
	})

	t.Run("unexpected file", func(t *testing.T) {
		t.Parallel()
		files := validFiles()
		files["polices.json"] = `{}` // the canonical typo
		files["notes.txt"] = "scratch"
		doc, findings := Validate(writeDir(t, files))
		assert.Nil(t, doc)
		out := findingStrings(findings)
		assert.Contains(t, out, "polices.json: unexpected file")
		assert.Contains(t, out, "notes.txt: unexpected file")
	})

	t.Run("unexpected directory", func(t *testing.T) {
		t.Parallel()
		dir := writeDir(t, validFiles())
		require.NoError(t, os.Mkdir(filepath.Join(dir, "backup"), 0o700))
		doc, findings := Validate(dir)
		assert.Nil(t, doc)
		assert.Contains(t, findingStrings(findings), "backup: unexpected directory")
	})

	t.Run("settings file is a directory", func(t *testing.T) {
		t.Parallel()
		files := validFiles()
		delete(files, FileRoles)
		dir := writeDir(t, files)
		require.NoError(t, os.Mkdir(filepath.Join(dir, FileRoles), 0o700))
		doc, findings := Validate(dir)
		assert.Nil(t, doc)
		out := findingStrings(findings)
		assert.Contains(t, out, "roles.json: is a directory")
		assert.NotContains(t, out, "missing", "one problem, one finding")
	})

	t.Run("unreadable file", func(t *testing.T) {
		t.Parallel()
		if os.Geteuid() == 0 {
			t.Skip("root ignores file permission bits")
		}
		dir := writeDir(t, validFiles())
		require.NoError(t, os.Chmod(filepath.Join(dir, FileConfig), 0o000))
		doc, findings := Validate(dir)
		assert.Nil(t, doc)
		out := findingStrings(findings)
		assert.Contains(t, out, "config.json: read:")
		assert.NotContains(t, out, "missing", "one problem, one finding")
	})

	t.Run("dot entries ignored", func(t *testing.T) {
		t.Parallel()
		// Editor swap files and the dot-prefixed machinery Kubernetes
		// ConfigMap mounts publish through (`..data` symlink directories) must
		// stay invisible — erroring on them would break the exact mount
		// pattern the cloud fan-out uses.
		files := validFiles()
		files[".roles.json.swp"] = "vim"
		dir := writeDir(t, files)
		require.NoError(t, os.Mkdir(filepath.Join(dir, "..data"), 0o700))
		doc, findings := Validate(dir)
		assert.Empty(t, findings)
		assert.NotNil(t, doc)
	})
}

func TestValidate_FileSyntax(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		file string
		body string
		want string
	}{
		{"empty file", FilePolicies, "", "file is empty"},
		{"whitespace only", FileRoles, "  \n\t", "file is empty"},
		{"byte order mark", FileRoles, "\xef\xbb\xbf{}", "byte order mark"},
		{"duplicate key inside an array element", FilePipes, `{"pipes": [{"name": "a", "sql": "SELECT 1", "name": "b"}]}`, "pipes[0].name: duplicate key"},
		{"null document", FileRoles, `null`, "document is null"},
		{"null with whitespace", FileConfig, "  null\n", "document is null"},
		{"syntax error", FilePipes, `{"pipes": [`, "unexpected EOF"},
		{"unknown field", FilePolicies, `{"default_roll": "public"}`, "unknown field"},
		{"trailing content", FileConfig, `{} {}`, "trailing content"},
		{"duplicate key", FileConfig, `{"dedupe": {"id_field": "a"}, "dedupe": {"id_field": "b"}}`, "dedupe: duplicate key"},
		{"nested duplicate key", FilePolicies, `{"tables": {"clicks": {"select": {"a": {}, "a": {}}}}}`, "tables.clicks.select.a: duplicate key"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			files := validFiles()
			files[tt.file] = tt.body
			doc, findings := Validate(writeDir(t, files))
			assert.Nil(t, doc)
			require.True(t, HasErrors(findings), "findings: %s", findingStrings(findings))
			assert.Contains(t, findingStrings(findings), tt.want)
			assert.Contains(t, findingStrings(findings), tt.file)
		})
	}
}

func TestValidate_ContentRules(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		file string
		body string
		want string
	}{
		{"empty role name", FileRoles, `{"roles": [""]}`, "roles[0]: role name must not be empty"},
		{"role whitespace", FileRoles, `{"roles": [" analyst"]}`, "surrounding whitespace"},
		{"duplicate role", FileRoles, `{"roles": ["analyst", "analyst"]}`, "duplicate role"},
		{"check uses _gt", FilePolicies, `{"tables": {"clicks": {"insert": {"analyst": {"check": {"region": {"_gt": "1"}}}}}}}`, "check does not honor"},
		{"operator-less filter", FilePolicies, `{"tables": {"clicks": {"select": {"analyst": {"filter": {"region": {}}}}}}}`, "sets no operator"},
		{"operator-less check", FilePolicies, `{"tables": {"clicks": {"insert": {"analyst": {"check": {"region": {}}}}}}}`, "sets no operator"},
		{"filter under insert", FilePolicies, `{"tables": {"clicks": {"insert": {"analyst": {"filter": {"region": {"_eq": "x"}}}}}}}`, "no effect on insert"},
		{"check under select", FilePolicies, `{"tables": {"clicks": {"select": {"analyst": {"check": {"region": {"_eq": "x"}}}}}}}`, "no effect on select"},
		{"unknown filter operator", FilePolicies, `{"tables": {"clicks": {"select": {"analyst": {"filter": {"region": {"_like": "x"}}}}}}}`, "unknown field"},
		{"empty grant role", FilePolicies, `{"default_role": "public", "tables": {"clicks": {"select": {"": {}}}}}`, "grant role must not be empty"},
		{"empty allowlist role", FilePipes, `{"pipes": [{"name": "a", "sql": "SELECT 1", "allowed_roles": [""]}]}`, "allowed_roles[0]: role must not be empty"},
		{"empty pipe name", FilePipes, `{"pipes": [{"name": "", "sql": "SELECT 1"}]}`, "pipe name must not be empty"},
		{"duplicate pipe", FilePipes, `{"pipes": [{"name": "a", "sql": "SELECT 1"}, {"name": "a", "sql": "SELECT 2"}]}`, "duplicate pipe"},
		{"empty pipe sql", FilePipes, `{"pipes": [{"name": "a", "sql": "  "}]}`, "pipe SQL must not be empty"},
		{"bad param type", FilePipes, `{"pipes": [{"name": "a", "sql": "SELECT 1", "parameters": [{"name": "x", "type": "float"}]}]}`, "unknown type"},
		{"duplicate param", FilePipes, `{"pipes": [{"name": "a", "sql": "SELECT 1", "parameters": [{"name": "x"}, {"name": "x"}]}]}`, "duplicate parameter"},
		{"empty dedupe id_field", FileConfig, `{"dedupe": {"id_field": ""}}`, "dedupe.id_field: must not be empty"},
		{"whitespace-only dedupe id_field", FileConfig, `{"dedupe": {"id_field": "  "}}`, "dedupe.id_field: must not be empty"},
		{"padded dedupe id_field", FileConfig, `{"dedupe": {"id_field": " click_id"}}`, "surrounding whitespace"},
		{"padded override id_field", FileConfig, `{"dedupe": {"tables": {"clicks": {"id_field": "click_id "}}}}`, "dedupe.tables.clicks.id_field"},
		{"empty override table name", FileConfig, `{"dedupe": {"tables": {"": {"id_field": "x"}}}}`, "table name must not be empty"},
		{"override table whitespace", FileConfig, `{"dedupe": {"tables": {" clicks": {"require_id": true}}}}`, "surrounding whitespace"},
		{"empty override id_field", FileConfig, `{"dedupe": {"tables": {"clicks": {"id_field": ""}}}}`, "dedupe.tables.clicks.id_field: must not be empty"},
		{"negative max rows", FileConfig, `{"query": {"default_max_rows": -1}}`, "must be >= 1"},
		{"zero max rows", FileConfig, `{"query": {"default_max_rows": 0}}`, "must be >= 1"},
		{"missing dedupe block", FileConfig, `{"dlq": {"enabled": true}, "query": {"default_max_rows": 1, "timestamp_bucket_seconds": 0}, "schema": {"refresh_interval": 1}, "stream": {"keepalive_interval": 1, "keepalive_buckets": 1, "gap_window_minutes": 0}, "mq": {"max_bytes_gb": 1}, "cors": {"allowed_origins": []}}`, "dedupe: required"},
		{"missing dlq block", FileConfig, `{"dedupe": {"enabled": false, "id_field": "event_id", "require_id": false}, "query": {"default_max_rows": 1, "timestamp_bucket_seconds": 0}, "schema": {"refresh_interval": 1}, "stream": {"keepalive_interval": 1, "keepalive_buckets": 1, "gap_window_minutes": 0}, "mq": {"max_bytes_gb": 1}, "cors": {"allowed_origins": []}}`, "dlq: required"},
		{"missing dlq.enabled", FileConfig, `{"dlq": {"tables": {}}}`, "dlq.enabled: required"},
		{"empty dlq override table name", FileConfig, `{"dlq": {"tables": {"": {"enabled": false}}}}`, "table name must not be empty"},
		{"dlq override table whitespace", FileConfig, `{"dlq": {"tables": {"clicks ": {"enabled": false}}}}`, "surrounding whitespace"},
		{"missing query.timestamp_bucket_seconds", FileConfig, `{"query": {"default_max_rows": 1}}`, "query.timestamp_bucket_seconds: required"},
		{"negative timestamp bucket", FileConfig, `{"query": {"timestamp_bucket_seconds": -1}}`, "must be >= 0"},
		{"missing stream block", FileConfig, `{"dedupe": {"enabled": false, "id_field": "event_id", "require_id": false}, "dlq": {"enabled": true}, "query": {"default_max_rows": 1, "timestamp_bucket_seconds": 0}, "schema": {"refresh_interval": 1}, "cors": {"allowed_origins": []}}`, "stream: required"},
		{"missing stream.keepalive_interval", FileConfig, `{"stream": {"keepalive_buckets": 3, "gap_window_minutes": 15}}`, "stream.keepalive_interval: required"},
		{"missing stream.keepalive_buckets", FileConfig, `{"stream": {"keepalive_interval": 30, "gap_window_minutes": 15}}`, "stream.keepalive_buckets: required"},
		{"missing stream.gap_window_minutes", FileConfig, `{"stream": {"keepalive_interval": 30, "keepalive_buckets": 3}}`, "stream.gap_window_minutes: required"},
		{"zero keepalive interval", FileConfig, `{"stream": {"keepalive_interval": 0}}`, "stream.keepalive_interval: must be >= 1"},
		{"zero keepalive buckets", FileConfig, `{"stream": {"keepalive_buckets": 0}}`, "stream.keepalive_buckets: must be >= 1"},
		{"negative gap window", FileConfig, `{"stream": {"gap_window_minutes": -1}}`, "stream.gap_window_minutes: must be >= 0"},
		{"keepalive as a duration string", FileConfig, `{"stream": {"keepalive_interval": "30s"}}`, "keepalive_interval"},
		{"missing dedupe.require_id", FileConfig, `{"dedupe": {"id_field": "event_id"}}`, "dedupe.require_id: required"},
		{"missing dedupe.enabled", FileConfig, `{"dedupe": {"id_field": "event_id", "require_id": false}}`, "dedupe.enabled: required"},
		{"missing query.default_max_rows", FileConfig, `{"query": {}}`, "query.default_max_rows: required"},
		{"missing schema.refresh_interval", FileConfig, `{"schema": {}}`, "schema.refresh_interval: required"},
		{"missing cors.allowed_origins", FileConfig, `{"cors": {}}`, "cors.allowed_origins: required"},
		{"empty config document", FileConfig, `{}`, "cors: required"},
		{"otel is boot config", FileConfig, `{"otel": {"enabled": true}}`, "unknown field"},
		{"missing clickhouse block", FileConfig, `{"auth": {"jwks_url": "", "role_claim": "role"}, "dedupe": {"enabled": false, "id_field": "event_id", "require_id": false}, "dlq": {"enabled": true}, "query": {"default_max_rows": 1, "timestamp_bucket_seconds": 0}, "schema": {"refresh_interval": 1}, "stream": {"keepalive_interval": 1, "keepalive_buckets": 1, "gap_window_minutes": 0}, "mq": {"max_bytes_gb": 1}, "cors": {"allowed_origins": []}}`, "clickhouse: required"},
		{"missing auth block", FileConfig, `{"clickhouse": {"addr": "h:9000", "http_port": 8123, "http_scheme": "http", "database": "d", "username": "u", "query_timeout": 1}, "dedupe": {"enabled": false, "id_field": "event_id", "require_id": false}, "dlq": {"enabled": true}, "query": {"default_max_rows": 1, "timestamp_bucket_seconds": 0}, "schema": {"refresh_interval": 1}, "stream": {"keepalive_interval": 1, "keepalive_buckets": 1, "gap_window_minutes": 0}, "mq": {"max_bytes_gb": 1}, "cors": {"allowed_origins": []}}`, "auth: required"},
		{"missing clickhouse.addr", FileConfig, `{"clickhouse": {"http_port": 8123, "http_scheme": "http", "database": "d", "username": "u", "query_timeout": 1}}`, "clickhouse.addr: required"},
		{"clickhouse.addr without port", FileConfig, `{"clickhouse": {"addr": "localhost"}}`, "must be host:port"},
		{"clickhouse.http_port out of range", FileConfig, `{"clickhouse": {"http_port": 70000}}`, "clickhouse.http_port: must be in 1-65535"},
		{"clickhouse.http_scheme ftp", FileConfig, `{"clickhouse": {"http_scheme": "ftp"}}`, `must be "http" or "https"`},
		{"clickhouse.database empty", FileConfig, `{"clickhouse": {"database": " "}}`, "clickhouse.database: must not be empty"},
		{"clickhouse.username empty", FileConfig, `{"clickhouse": {"username": ""}}`, "clickhouse.username: must not be empty"},
		{"clickhouse.query_timeout zero", FileConfig, `{"clickhouse": {"query_timeout": 0}}`, "clickhouse.query_timeout: must be >= 1"},
		{"missing auth.jwks_url", FileConfig, `{"auth": {"role_claim": "role"}}`, "auth.jwks_url: required"},
		{"missing auth.role_claim", FileConfig, `{"auth": {"jwks_url": ""}}`, "auth.role_claim: required"},
		{"auth.jwks_url relative", FileConfig, `{"auth": {"jwks_url": "/.well-known/jwks.json"}}`, "must be an absolute http(s) URL"},
		{"auth.jwks_url bad scheme", FileConfig, `{"auth": {"jwks_url": "ftp://idp.example/jwks"}}`, "must be an absolute http(s) URL"},
		{"auth.role_claim empty", FileConfig, `{"auth": {"role_claim": ""}}`, "auth.role_claim: must be a non-empty claim path"},
		{"auth.role_claim padded", FileConfig, `{"auth": {"role_claim": " role"}}`, "auth.role_claim: must be a non-empty claim path"},

		{"zero refresh interval", FileConfig, `{"schema": {"refresh_interval": 0}}`, "must be >= 1"},
		{"missing mq.max_bytes_gb", FileConfig, `{"mq": {}}`, "mq.max_bytes_gb: required"},
		{"zero mq.max_bytes_gb", FileConfig, `{"mq": {"max_bytes_gb": 0}}`, "mq.max_bytes_gb: must be >= 1 GB"},
		{"empty cors origin", FileConfig, `{"cors": {"allowed_origins": [" "]}}`, "origin must not be empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			files := validFiles()
			files[tt.file] = tt.body
			doc, findings := Validate(writeDir(t, files))
			assert.Nil(t, doc)
			require.True(t, HasErrors(findings), "findings: %s", findingStrings(findings))
			assert.Contains(t, findingStrings(findings), tt.want)
		})
	}
}

func TestValidate_RoleReferences(t *testing.T) {
	t.Parallel()

	t.Run("undeclared roles are errors", func(t *testing.T) {
		t.Parallel()
		files := validFiles()
		files[FilePolicies] = `{"default_role": "ghost", "tables": {"clicks": {"select": {"phantom": {}}}}}`
		files[FilePipes] = `{"pipes": [{"name": "a", "sql": "SELECT 1", "allowed_roles": ["specter"]}]}`
		doc, findings := Validate(writeDir(t, files))
		assert.Nil(t, doc)
		out := findingStrings(findings)
		assert.Contains(t, out, `default_role: role "ghost" is not declared`)
		assert.Contains(t, out, `tables.clicks.select.phantom: role "phantom" is not declared`)
		assert.Contains(t, out, `pipes[0].allowed_roles[0]: role "specter" is not declared`)
	})

	t.Run("undeclared admin_role is an error", func(t *testing.T) {
		t.Parallel()
		files := validFiles()
		files[FilePolicies] = `{"admin_role": "root", "tables": {}}`
		doc, findings := Validate(writeDir(t, files))
		assert.Nil(t, doc)
		assert.Contains(t, findingStrings(findings), `admin_role: role "root" is not declared`)
	})

	t.Run("skipped when roles.json is broken", func(t *testing.T) {
		t.Parallel()
		files := validFiles()
		files[FileRoles] = `{"roles": [`
		_, findings := Validate(writeDir(t, files))
		out := findingStrings(findings)
		assert.NotContains(t, out, "is not declared", "reference checks against a broken registry are noise")
	})
}

func TestValidate_Warnings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		file string
		body string
		want string
	}{
		{"admin grant is dead config", FilePolicies, `{"default_role": "public", "tables": {"clicks": {"select": {"admin": {"max_rows": 5}}}}}`, "unconditional bypass"},
		{"default_role equals admin", FilePolicies, `{"default_role": "admin", "tables": {}}`, "every roleless request gets full admin"},
		{"admin in pipe allowlist is redundant", FilePipes, `{"pipes": [{"name": "a", "sql": "SELECT 1", "allowed_roles": ["admin"]}]}`, "listing it is redundant"},
		{"empty dedupe override sets nothing", FileConfig, configJSON(`{"dedupe": {"tables": {"clicks": {}}}}`), "override sets nothing"},
		{"empty dlq override sets nothing", FileConfig, configJSON(`{"dlq": {"tables": {"clicks": {}}}}`), "dlq.tables.clicks: override sets nothing"},

		{"empty cors allowlist allows every origin", FileConfig, configJSON(`{"cors": {"allowed_origins": []}}`), "empty list allows every origin"},
		{"default on required parameter", FilePipes, `{"pipes": [{"name": "a", "sql": "SELECT 1", "parameters": [{"name": "x", "required": true, "default": 5}]}]}`, "never used"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			files := validFiles()
			files[tt.file] = tt.body
			doc, findings := Validate(writeDir(t, files))
			require.NotNil(t, doc, "warnings alone must leave the directory valid: %s", findingStrings(findings))
			assert.False(t, HasErrors(findings))
			assert.Contains(t, findingStrings(findings), tt.want)
		})
	}
}

// TestValidate_MultipleFaults pins the one-pass contract head-on: independent
// problems in different files are all reported together — a syntax error in
// one file must not suppress content or reference findings in another — and
// exactly once, so the operator gets the whole list without noise.
func TestValidate_MultipleFaults(t *testing.T) {
	t.Parallel()
	doc, findings := Validate(writeDir(t, map[string]string{
		FileRoles:    `{"roles": ["analyst", "analyst"]}`,               // duplicate role
		FilePolicies: `{"default_role": "ghost", "tables": {}}`,         // undeclared role
		FilePipes:    `{"pipes": [`,                                     // truncated JSON
		FileConfig:   configJSON(`{"query": {"default_max_rows": -1}}`), // bounds violation
	}))
	assert.Nil(t, doc)
	out := findingStrings(findings)
	assert.Contains(t, out, "duplicate role")
	assert.Contains(t, out, `role "ghost" is not declared`)
	assert.Contains(t, out, "unexpected EOF")
	assert.Contains(t, out, "must be >= 1")
	assert.Len(t, findings, 4, "every fault reported exactly once, no noise:\n%s", out)
}

// TestValidate_ErrorAndWarningMix pins that a warning is still reported
// alongside an error, and that the error alone decides rejection.
func TestValidate_ErrorAndWarningMix(t *testing.T) {
	t.Parallel()
	files := validFiles()
	files[FilePolicies] = `{"default_role": "public", "tables": {"clicks": {"select": {"admin": {}}}}}` // warning: admin grant is dead config
	files[FileConfig] = configJSON(`{"schema": {"refresh_interval": 0}}`)                               // error: bounds violation
	doc, findings := Validate(writeDir(t, files))
	assert.Nil(t, doc, "one error rejects the directory even when the rest only warns")
	require.True(t, HasErrors(findings))
	out := findingStrings(findings)
	assert.Contains(t, out, "unconditional bypass")
	assert.Contains(t, out, "must be >= 1")
	assert.Len(t, findings, 2, "findings:\n%s", out)
}

// FuzzSyntaxGate pins that the hand-rolled pieces of the syntax gate — strict
// decoding and the token-level duplicate-key scan — never panic on arbitrary
// bytes. Hand-edited files are exactly where arbitrary bytes come from.
func FuzzSyntaxGate(f *testing.F) {
	for _, seed := range []string{
		"", "null", "{}", "[]", `{"a":1,"a":2}`, `{"a":[{"b":1,"b":2}]}`,
		`{"pipes": [`, "\xef\xbb\xbf{}", `{"a":"\ud800"}`, `[[[[[[[[`, `{"a"`,
		strings.Repeat(`{"a":`, 100) + "1" + strings.Repeat("}", 100),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, body string) {
		var roles RolesFile
		_ = decodeStrict([]byte(body), &roles)
		_ = dupKeyFindings(FileRoles, []byte(body))
	})
}

// TestValidate_DocumentGating pins the contract: a Document comes back only
// when no finding is an error.
func TestValidate_DocumentGating(t *testing.T) {
	t.Parallel()
	doc, findings := Validate(writeDir(t, validFiles()))
	assert.NotNil(t, doc)
	assert.Empty(t, findings)
	doc, findings = Validate(filepath.Join(t.TempDir(), "nope"))
	assert.Nil(t, doc)
	assert.True(t, HasErrors(findings))
}

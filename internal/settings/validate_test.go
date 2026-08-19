package settings

import (
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
		FileConfig:   `{"dedupe": {"id_field": "event_id", "tables": {"clicks": {"id_field": "click_id"}}}, "schema": {"refresh_interval": 60}}`,
	}
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
	doc, findings := parse(writeDir(t, validFiles()))

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
	doc, findings := parse(writeDir(t, map[string]string{
		FileRoles: `{}`, FilePolicies: `{}`, FilePipes: `{}`, FileConfig: `{}`,
	}))

	require.Empty(t, findings)
	require.NotNil(t, doc)
	assert.Nil(t, doc.Policy, "an empty policies.json means no policy (fail closed)")
	assert.Empty(t, doc.Roles)
	assert.Empty(t, doc.Pipes)
}

func TestValidate_DirectoryProblems(t *testing.T) {
	t.Parallel()

	t.Run("missing directory", func(t *testing.T) {
		t.Parallel()
		doc, findings := parse(filepath.Join(t.TempDir(), "nope"))
		assert.Nil(t, doc)
		require.True(t, HasErrors(findings))
		assert.Contains(t, findingStrings(findings), "does not exist")
	})

	t.Run("path is a file", func(t *testing.T) {
		t.Parallel()
		f := filepath.Join(t.TempDir(), "file")
		require.NoError(t, os.WriteFile(f, []byte("x"), 0o600))
		doc, findings := parse(f)
		assert.Nil(t, doc)
		assert.Contains(t, findingStrings(findings), "not a directory")
	})

	t.Run("missing file", func(t *testing.T) {
		t.Parallel()
		files := validFiles()
		delete(files, FileConfig)
		doc, findings := parse(writeDir(t, files))
		assert.Nil(t, doc)
		assert.Contains(t, findingStrings(findings), "config.json: missing")
	})

	t.Run("unknown json file", func(t *testing.T) {
		t.Parallel()
		files := validFiles()
		files["polices.json"] = `{}`
		doc, findings := parse(writeDir(t, files))
		assert.Nil(t, doc)
		assert.Contains(t, findingStrings(findings), "polices.json: unknown settings file")
	})

	t.Run("non-json entries ignored", func(t *testing.T) {
		t.Parallel()
		files := validFiles()
		files["notes.txt"] = "scratch"
		files[".roles.json.swp"] = "vim"
		doc, findings := parse(writeDir(t, files))
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
			doc, findings := parse(writeDir(t, files))
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
		{"unknown filter operator", FilePolicies, `{"tables": {"clicks": {"select": {"analyst": {"filter": {"region": {"_like": "x"}}}}}}}`, "unknown field"},
		{"empty pipe name", FilePipes, `{"pipes": [{"name": "", "sql": "SELECT 1"}]}`, "pipe name must not be empty"},
		{"duplicate pipe", FilePipes, `{"pipes": [{"name": "a", "sql": "SELECT 1"}, {"name": "a", "sql": "SELECT 2"}]}`, "duplicate pipe"},
		{"empty pipe sql", FilePipes, `{"pipes": [{"name": "a", "sql": "  "}]}`, "pipe SQL must not be empty"},
		{"bad param type", FilePipes, `{"pipes": [{"name": "a", "sql": "SELECT 1", "parameters": [{"name": "x", "type": "float"}]}]}`, "unknown type"},
		{"duplicate param", FilePipes, `{"pipes": [{"name": "a", "sql": "SELECT 1", "parameters": [{"name": "x"}, {"name": "x"}]}]}`, "duplicate parameter"},
		{"empty dedupe id_field", FileConfig, `{"dedupe": {"id_field": ""}}`, "dedupe.id_field: must not be empty"},
		{"empty override table name", FileConfig, `{"dedupe": {"tables": {"": {"id_field": "x"}}}}`, "table name must not be empty"},
		{"override table whitespace", FileConfig, `{"dedupe": {"tables": {" clicks": {"require_id": true}}}}`, "surrounding whitespace"},
		{"empty override id_field", FileConfig, `{"dedupe": {"tables": {"clicks": {"id_field": ""}}}}`, "dedupe.tables.clicks.id_field: must not be empty"},
		{"negative max rows", FileConfig, `{"query": {"default_max_rows": -1}}`, "must be non-negative"},
		{"stream is boot config", FileConfig, `{"stream": {"keepalive_interval": "30s"}}`, "unknown field"},
		{"zero refresh interval", FileConfig, `{"schema": {"refresh_interval": 0}}`, "must be >= 1"},
		{"empty cors origin", FileConfig, `{"cors": {"allowed_origins": [" "]}}`, "origin must not be empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			files := validFiles()
			files[tt.file] = tt.body
			doc, findings := parse(writeDir(t, files))
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
		doc, findings := parse(writeDir(t, files))
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
		doc, findings := parse(writeDir(t, files))
		assert.Nil(t, doc)
		assert.Contains(t, findingStrings(findings), `admin_role: role "root" is not declared`)
	})

	t.Run("skipped when roles.json is broken", func(t *testing.T) {
		t.Parallel()
		files := validFiles()
		files[FileRoles] = `{"roles": [`
		_, findings := parse(writeDir(t, files))
		out := findingStrings(findings)
		assert.NotContains(t, out, "is not declared", "reference checks against a broken registry are noise")
	})
}

func TestValidate_Warnings(t *testing.T) {
	t.Parallel()

	t.Run("admin grant is dead config", func(t *testing.T) {
		t.Parallel()
		files := validFiles()
		files[FilePolicies] = `{"default_role": "public", "tables": {"clicks": {"select": {"admin": {"max_rows": 5}}}}}`
		doc, findings := parse(writeDir(t, files))
		require.NotNil(t, doc, "warnings alone must leave the directory valid: %s", findingStrings(findings))
		assert.False(t, HasErrors(findings))
		assert.Contains(t, findingStrings(findings), "unconditional bypass")
	})

	t.Run("default_role equals admin", func(t *testing.T) {
		t.Parallel()
		files := validFiles()
		files[FilePolicies] = `{"default_role": "admin", "tables": {}}`
		doc, findings := parse(writeDir(t, files))
		require.NotNil(t, doc)
		assert.Contains(t, findingStrings(findings), "every roleless request gets full admin")
	})

	t.Run("admin in pipe allowlist is redundant", func(t *testing.T) {
		t.Parallel()
		files := validFiles()
		files[FilePipes] = `{"pipes": [{"name": "a", "sql": "SELECT 1", "allowed_roles": ["admin"]}]}`
		doc, findings := parse(writeDir(t, files))
		require.NotNil(t, doc)
		assert.Contains(t, findingStrings(findings), "listing it is redundant")
	})

	t.Run("empty dedupe override sets nothing", func(t *testing.T) {
		t.Parallel()
		files := validFiles()
		files[FileConfig] = `{"dedupe": {"tables": {"clicks": {}}}}`
		doc, findings := parse(writeDir(t, files))
		require.NotNil(t, doc, "an empty override is a warning, not an error")
		assert.Contains(t, findingStrings(findings), "override sets nothing")
	})

	t.Run("default on required parameter", func(t *testing.T) {
		t.Parallel()
		files := validFiles()
		files[FilePipes] = `{"pipes": [{"name": "a", "sql": "SELECT 1", "parameters": [{"name": "x", "required": true, "default": 5}]}]}`
		doc, findings := parse(writeDir(t, files))
		require.NotNil(t, doc)
		assert.Contains(t, findingStrings(findings), "never used")
	})
}

// TestValidate_FindingsOnly pins the exported surface: Validate checks and
// reports — no parsed data escapes it.
func TestValidate_FindingsOnly(t *testing.T) {
	t.Parallel()
	assert.Empty(t, Validate(writeDir(t, validFiles())))
	assert.True(t, HasErrors(Validate(filepath.Join(t.TempDir(), "nope"))))
}

func TestFinding_String(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "error: policies.json: default_role: boom",
		Finding{Severity: SeverityError, File: FilePolicies, Path: "default_role", Message: "boom"}.String())
	assert.Equal(t, "error: settings directory missing",
		Finding{Severity: SeverityError, Message: "settings directory missing"}.String())
	assert.Equal(t, "warning: pipes.json: shadowed",
		Finding{Severity: SeverityWarning, File: FilePipes, Message: "shadowed"}.String())
}

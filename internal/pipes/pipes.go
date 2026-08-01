package pipes

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/Wave-RF/WaveHouse/internal/controldb"
	"github.com/google/uuid"
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

// Store manages named query persistence in the control-plane database with
// optional file bootstrap. A NamedQuery is decomposed into a pipes row plus
// pipe_params and pipe_roles child rows on Put and recomposed on load; the
// in-memory cache is the read path, so no request touches SQLite.
type Store struct {
	db     *sql.DB
	logger *slog.Logger

	// writeMu serializes writers across the database write AND the cache
	// mutation that follows, so the cache always lands in database-commit
	// order — without it a racing Put/Delete pair can leave the cache holding
	// the losing document until the next boot. Readers never take it.
	writeMu sync.Mutex

	mu     sync.RWMutex
	cached map[string]*NamedQuery
}

// NewStore creates a pipes store backed by the control-plane database.
// If directory is non-empty, .sql files in it are loaded on startup (a seed:
// a pipe name already in the database is never overwritten).
func NewStore(ctx context.Context, db *sql.DB, directory string, logger *slog.Logger) (*Store, error) {
	s := &Store{db: db, logger: logger, cached: make(map[string]*NamedQuery)}

	// Bootstrap from SQL files.
	if directory != "" {
		if err := s.loadFromDirectory(ctx, directory); err != nil {
			logger.Warn("pipes directory load failed", "dir", directory, "error", err)
		}
	}

	// Load all stored pipes into the cache. Unlike the old KV backend this is
	// fatal: the control db is a local file, so a read failure means a broken
	// deployment, and refusing boot beats silently serving zero pipes.
	if err := s.refresh(ctx); err != nil {
		return nil, fmt.Errorf("load pipes from control db: %w", err)
	}

	return s, nil
}

// Get returns a named query by name, or nil if not found.
func (s *Store) Get(name string) *NamedQuery {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cached[name]
}

// List returns all cached named queries.
func (s *Store) List() []*NamedQuery {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*NamedQuery, 0, len(s.cached))
	for _, q := range s.cached {
		result = append(result, q)
	}
	return result
}

// Put saves a named query: the pipes row is upserted by name (the surrogate
// id survives an update, so a future rename surface keeps child rows), and
// the parameter and role child rows are fully replaced — all in one
// transaction.
func (s *Store) Put(ctx context.Context, q *NamedQuery) error {
	if q.Name == "" {
		return fmt.Errorf("pipe name is required")
	}
	if q.SQL == "" {
		return fmt.Errorf("pipe SQL is required")
	}
	// Reject rather than dedupe: a silently-dropped declaration would make
	// the cached document (full slice) and the stored rows (survivors only)
	// resolve differently across a restart.
	seen := make(map[string]bool, len(q.Parameters))
	for _, p := range q.Parameters {
		if p.Name == "" {
			return fmt.Errorf("pipe %q: parameter name is required", q.Name)
		}
		if seen[p.Name] {
			return fmt.Errorf("pipe %q: duplicate parameter %q", q.Name, p.Name)
		}
		seen[p.Name] = true
	}
	// Roles get normalization, not rejection: allowed_roles is a set (empty
	// entries authorize nobody, duplicates are idempotent — see RoleAllowed),
	// so dropping them here keeps the lenient API contract while making the
	// cached document identical to the stored rows across a restart.
	q.AllowedRoles = normalizeRoles(q.AllowedRoles)

	// One writer at a time: persist and the cache update must apply in the
	// same order (see writeMu).
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if err := s.persist(ctx, q); err != nil {
		return err
	}

	s.mu.Lock()
	s.cached[q.Name] = q
	s.mu.Unlock()

	s.logger.Info("pipe saved", "name", q.Name)
	return nil
}

// persist writes the decomposed pipe inside one transaction. The pipes row
// is one insert-or-update statement (the no-op on conflict keeps the existing
// surrogate id, which RETURNING yields either way); child rows are replaced
// wholesale — a PUT is the full document.
func (s *Store) persist(ctx context.Context, q *NamedQuery) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin pipe write: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var pipeID string
	if err := tx.QueryRowContext(ctx, `INSERT INTO pipes (id, name, sql, description) VALUES (?, ?, ?, ?)
		ON CONFLICT (name) DO UPDATE SET sql = excluded.sql, description = excluded.description
		RETURNING id`, uuid.NewString(), q.Name, q.SQL, q.Description).Scan(&pipeID); err != nil {
		return fmt.Errorf("upsert pipe %q: %w", q.Name, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM pipe_params WHERE pipe_id = ?`, pipeID); err != nil {
		return fmt.Errorf("clear pipe params: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM pipe_roles WHERE pipe_id = ?`, pipeID); err != nil {
		return fmt.Errorf("clear pipe roles: %w", err)
	}

	for i, p := range q.Parameters {
		var defaultVal any
		if p.Default != nil {
			data, err := json.Marshal(p.Default)
			if err != nil {
				return fmt.Errorf("marshal default for param %q: %w", p.Name, err)
			}
			defaultVal = string(data)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO pipe_params
			(pipe_id, param_name, position, type, required, default_val) VALUES (?, ?, ?, ?, ?, ?)`,
			pipeID, p.Name, i, p.Type, p.Required, defaultVal); err != nil {
			return fmt.Errorf("insert pipe param %q: %w", p.Name, err)
		}
	}

	for i, roleName := range q.AllowedRoles {
		roleID, err := controldb.UpsertRole(ctx, tx, roleName)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO pipe_roles (pipe_id, role_id, position) VALUES (?, ?, ?)`,
			pipeID, roleID, i); err != nil {
			return fmt.Errorf("insert pipe role %q: %w", roleName, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit pipe write: %w", err)
	}
	return nil
}

// normalizeRoles drops empty and duplicate entries, preserving first-seen
// order; nil when nothing survives (matching loadRoles' shape for "none").
func normalizeRoles(roles []string) []string {
	var out []string
	seen := make(map[string]bool, len(roles))
	for _, r := range roles {
		if r == "" || seen[r] {
			continue
		}
		seen[r] = true
		out = append(out, r)
	}
	return out
}

// Delete removes a named query from the store; deleting the pipes row
// cascades its parameter and role rows. Deleting an absent pipe is a no-op
// success (idempotent DELETE, the same contract as the KV backend this
// replaced).
func (s *Store) Delete(ctx context.Context, name string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if _, err := s.db.ExecContext(ctx, `DELETE FROM pipes WHERE name = ?`, name); err != nil {
		return fmt.Errorf("delete pipe: %w", err)
	}

	s.mu.Lock()
	delete(s.cached, name)
	s.mu.Unlock()

	s.logger.Info("pipe deleted", "name", name)
	return nil
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

// refresh recomposes every stored pipe from its rows into the cache.
func (s *Store) refresh(ctx context.Context) error {
	cached := make(map[string]*NamedQuery)

	rows, err := s.db.QueryContext(ctx, `SELECT id, name, sql, description FROM pipes`)
	if err != nil {
		return fmt.Errorf("read pipes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	idsByName := map[string]string{}
	for rows.Next() {
		var id, name, sqlText, description string
		if err := rows.Scan(&id, &name, &sqlText, &description); err != nil {
			return fmt.Errorf("scan pipe: %w", err)
		}
		idsByName[name] = id
		cached[name] = &NamedQuery{Name: name, SQL: sqlText, Description: description}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read pipes: %w", err)
	}

	for name, id := range idsByName {
		q := cached[name]
		if q.Parameters, err = s.loadParams(ctx, id); err != nil {
			return fmt.Errorf("pipe %q: %w", name, err)
		}
		if q.AllowedRoles, err = s.loadRoles(ctx, id); err != nil {
			return fmt.Errorf("pipe %q: %w", name, err)
		}
	}

	s.mu.Lock()
	s.cached = cached
	s.mu.Unlock()
	return nil
}

// loadParams recomposes a pipe's parameter list in declaration order; nil
// when the pipe declares none (matching the omitempty JSON shape).
func (s *Store) loadParams(ctx context.Context, pipeID string) ([]ParamDef, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT param_name, type, required, default_val FROM pipe_params WHERE pipe_id = ? ORDER BY position`, pipeID)
	if err != nil {
		return nil, fmt.Errorf("read pipe params: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var params []ParamDef
	for rows.Next() {
		var p ParamDef
		var defaultVal sql.NullString
		if err := rows.Scan(&p.Name, &p.Type, &p.Required, &defaultVal); err != nil {
			return nil, fmt.Errorf("scan pipe param: %w", err)
		}
		if defaultVal.Valid {
			if err := json.Unmarshal([]byte(defaultVal.String), &p.Default); err != nil {
				return nil, fmt.Errorf("unmarshal default for param %q: %w", p.Name, err)
			}
		}
		params = append(params, p)
	}
	return params, rows.Err()
}

// loadRoles recomposes a pipe's allowed-role names in declaration order; nil
// when the pipe has none (admin-only, fails closed).
func (s *Store) loadRoles(ctx context.Context, pipeID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT r.name FROM pipe_roles pr
		JOIN roles r ON r.id = pr.role_id WHERE pr.pipe_id = ? ORDER BY pr.position`, pipeID)
	if err != nil {
		return nil, fmt.Errorf("read pipe roles: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var roles []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan pipe role: %w", err)
		}
		roles = append(roles, name)
	}
	return roles, rows.Err()
}

// loadFromDirectory scans a directory for .sql files and seeds them into the
// database. A pipe name that already exists is never overwritten.
func (s *Store) loadFromDirectory(ctx context.Context, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".sql")
		// #nosec G304 -- dir is operator-configured; filename comes from os.ReadDir of that dir.
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			s.logger.Warn("failed to read pipe file", "file", entry.Name(), "error", err)
			continue
		}
		// Check if already stored — don't overwrite.
		var exists int
		err = s.db.QueryRowContext(ctx, `SELECT 1 FROM pipes WHERE name = ?`, name).Scan(&exists)
		if err == nil {
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("look up pipe %q: %w", name, err)
		}
		q := &NamedQuery{
			Name: name,
			SQL:  string(data),
		}
		if err := s.Put(ctx, q); err != nil {
			s.logger.Warn("failed to bootstrap pipe", "name", name, "error", err)
		}
	}
	return nil
}

// NewMemoryStore creates a pipes store over an in-memory control database,
// pre-loaded with the given queries. Intended for testing: same engine, same
// constraints, same code paths as production — only the storage lives in RAM.
// Seeds populate the cache directly (a later Put replaces that pipe's rows
// wholesale, and Delete of a never-persisted name is a no-op).
func NewMemoryStore(queries ...*NamedQuery) *Store {
	cached := make(map[string]*NamedQuery, len(queries))
	for _, q := range queries {
		cached[q.Name] = q
	}
	return &Store{
		db:     controldb.MustOpenMemory(),
		cached: cached,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

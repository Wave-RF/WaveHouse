package pipes

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
)

const kvBucket = "WAVEHOUSE_PIPES"

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
	// Type is enforced when set: a supplied value of the wrong kind is a 400.
	// "string", "number", "boolean" (scalar), or "array" (a list, e.g. for IN).
	// An empty or unrecognized type imposes no constraint.
	Type     string `json:"type"`
	Required bool   `json:"required,omitempty"`
	Default  any    `json:"default,omitempty"`
}

// Store manages named query persistence via NATS KV with optional file bootstrap.
type Store struct {
	kv     jetstream.KeyValue
	logger *slog.Logger
	mu     sync.RWMutex
	cached map[string]*NamedQuery
}

// NewStore creates a pipes store backed by NATS KV.
// If directory is non-empty, .sql files in it are loaded on startup.
func NewStore(ctx context.Context, js jetstream.JetStream, directory string, logger *slog.Logger) (*Store, error) {
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  kvBucket,
		History: 3,
	})
	if err != nil {
		return nil, fmt.Errorf("create pipes kv bucket: %w", err)
	}

	s := &Store{kv: kv, logger: logger, cached: make(map[string]*NamedQuery)}

	// Bootstrap from SQL files.
	if directory != "" {
		if err := s.loadFromDirectory(ctx, directory); err != nil {
			logger.Warn("pipes directory load failed", "dir", directory, "error", err)
		}
	}

	// Load all existing pipes from KV into cache.
	if err := s.refresh(ctx); err != nil {
		logger.Warn("pipes initial cache load failed", "error", err)
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

// Put saves a named query to the NATS KV store.
func (s *Store) Put(ctx context.Context, q *NamedQuery) error {
	if q.Name == "" {
		return fmt.Errorf("pipe name is required")
	}
	if q.SQL == "" {
		return fmt.Errorf("pipe SQL is required")
	}

	data, err := json.Marshal(q)
	if err != nil {
		return fmt.Errorf("marshal pipe: %w", err)
	}

	if s.kv != nil {
		if _, err := s.kv.Put(ctx, q.Name, data); err != nil {
			return fmt.Errorf("put pipe to kv: %w", err)
		}
	}

	s.mu.Lock()
	s.cached[q.Name] = q
	s.mu.Unlock()

	s.logger.Info("pipe saved", "name", q.Name)
	return nil
}

// Delete removes a named query from the store.
func (s *Store) Delete(ctx context.Context, name string) error {
	if s.kv != nil {
		if err := s.kv.Delete(ctx, name); err != nil {
			return fmt.Errorf("delete pipe: %w", err)
		}
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

// isNumericLiteral reports whether s is a plain SQL numeric literal. It is the
// single source of truth for "looks like a number" shared by validateParamType
// (a declared "number" param accepts only these; anything else is a clean 400)
// and formatParamValue (only these render bare — any other string is quoted, so
// a numeric-looking value can never become invalid bare SQL).
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
// driver-level positional parameter limitations (e.g. LIMIT position). A
// declared ParamDef.Type is enforced before formatting.
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

		// Enforce the declared type (if any) before formatting, so a parameter
		// declared scalar rejects a non-scalar value rather than silently
		// rendering it as a list. The declared type also reaches the formatter,
		// so a declared string always renders as a quoted literal rather than
		// taking the bare numeric shortcut.
		declaredType := ""
		if p, ok := formal[name]; ok {
			declaredType = p.Type
		}
		if declaredType != "" {
			if err := validateParamType(declaredType, val); err != nil {
				bindErr = fmt.Errorf("parameter %q: %w", name, err)
				return match
			}
		}

		lit, err := formatParamValue(val, declaredType)
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
//
// declaredType is the parameter's declared ParamDef.Type ("" for inline or
// untyped parameters). When it is "string", a numeric-looking value is quoted
// rather than emitted bare, so a parameter declared a string always renders as
// a string literal. When it is "boolean", a string spelling from the query
// string ("true"/"false") renders as the numeric 1/0 a Bool column expects,
// matching a JSON-body bool. Array elements carry no declared element type and
// are formatted with an empty declaredType.
func formatParamValue(v any, declaredType string) (string, error) {
	if v == nil {
		return "NULL", nil
	}
	switch val := v.(type) {
	case string:
		// A boolean declared via the query string arrives as "true"/"false";
		// render it as the numeric 1/0 a Bool column expects, matching the
		// JSON-body bool case below. validateParamType guarantees the string
		// parses, but fall through to a quoted literal if it somehow doesn't.
		if declaredType == "boolean" {
			if b, err := strconv.ParseBool(val); err == nil {
				if b {
					return "1", nil
				}
				return "0", nil
			}
		}
		// A numeric-looking string renders bare (so `?limit=100` from a query
		// string works as a number), unless the parameter is declared a string —
		// then it is always quoted, honoring the declared type.
		if declaredType != "string" && isNumericLiteral(val) {
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
			s, err := formatParamValue(elem, "")
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

// validateParamType checks a supplied value against a parameter's declared
// type. Only the recognized types ("string", "number", "boolean", "array")
// are enforced; an unrecognized type imposes no constraint (formatParamValue
// still guarantees the rendered literal is safe). Values from the query string
// always arrive as strings — even for number and boolean parameters — so those
// checks accept the string spelling too.
func validateParamType(declared string, v any) error {
	if v == nil {
		return nil // renders as NULL, valid for any column type
	}
	switch declared {
	case "string":
		// Query-string and JSON-string values both arrive as Go strings; a JSON
		// number/boolean/array/object is a declared-type violation.
		if _, ok := v.(string); !ok {
			return fmt.Errorf("expected string, got %s", jsonKind(v))
		}
		return nil
	case "number":
		switch n := v.(type) {
		case float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return nil
		case string:
			if isNumericLiteral(n) {
				return nil
			}
		}
		return fmt.Errorf("expected number, got %s", jsonKind(v))
	case "boolean":
		switch b := v.(type) {
		case bool:
			return nil
		case string:
			if _, err := strconv.ParseBool(b); err == nil {
				return nil
			}
		}
		return fmt.Errorf("expected boolean, got %s", jsonKind(v))
	case "array":
		if _, ok := v.([]any); ok {
			return nil
		}
		return fmt.Errorf("expected array, got %s", jsonKind(v))
	default:
		return nil // unrecognized declared type: not enforced
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

// refresh reloads all pipes from NATS KV into the cache.
func (s *Store) refresh(ctx context.Context) error {
	keys, err := s.kv.Keys(ctx)
	if err != nil {
		return err
	}
	cached := make(map[string]*NamedQuery, len(keys))
	for _, key := range keys {
		entry, err := s.kv.Get(ctx, key)
		if err != nil {
			continue
		}
		var q NamedQuery
		if err := json.Unmarshal(entry.Value(), &q); err != nil {
			continue
		}
		cached[key] = &q
	}
	s.mu.Lock()
	s.cached = cached
	s.mu.Unlock()
	return nil
}

// loadFromDirectory scans a directory for .sql files and bootstraps them into KV.
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
		// Check if already exists in KV — don't overwrite.
		if _, err := s.kv.Get(ctx, name); err == nil {
			continue
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

// NewMemoryStore creates an in-memory pipes store for testing without NATS.
func NewMemoryStore(queries ...*NamedQuery) *Store {
	cached := make(map[string]*NamedQuery, len(queries))
	for _, q := range queries {
		cached[q.Name] = q
	}
	return &Store{
		cached: cached,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

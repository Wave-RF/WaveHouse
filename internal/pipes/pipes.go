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
	AllowedRoles []string   `json:"allowed_roles,omitempty"` // empty = all roles
}

// ParamDef describes a query parameter.
type ParamDef struct {
	Name     string `json:"name"`
	Type     string `json:"type"` // "string", "number", "boolean"
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

// BindParams replaces {{param}} and {{param:default}} placeholders in a NamedQuery's SQL
// with supplied values or defaults. Formally declared Parameters provide type info and
// required/default metadata. Inline {{name:default}} syntax also works without formal
// parameter definitions.
//
// Values are inlined directly into the SQL string (strings are single-quote escaped).
// This avoids driver-level positional parameter limitations (e.g. LIMIT position).
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

		return formatParamValue(val)
	})

	if bindErr != nil {
		return "", nil, bindErr
	}
	return sql, nil, nil
}

// formatParamValue converts a Go value to a safe SQL literal for inline substitution.
func formatParamValue(v any) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		// If the string looks like a number (common with inline defaults), return it bare.
		if _, err := strconv.ParseFloat(val, 64); err == nil {
			return val
		}
		escaped := strings.ReplaceAll(val, `\`, `\\`)
		escaped = strings.ReplaceAll(escaped, `'`, `''`)
		return "'" + escaped + "'"
	case float64:
		// JSON numbers are float64; if it's a whole number, format as integer.
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	case float32:
		if val == float32(int32(val)) {
			return fmt.Sprintf("%d", int32(val))
		}
		return fmt.Sprintf("%g", val)
	case bool:
		if val {
			return "1"
		}
		return "0"
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", val)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val)
	default:
		return fmt.Sprintf("%v", val)
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

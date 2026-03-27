package pipes

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
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

	if _, err := s.kv.Put(ctx, q.Name, data); err != nil {
		return fmt.Errorf("put pipe to kv: %w", err)
	}

	s.mu.Lock()
	s.cached[q.Name] = q
	s.mu.Unlock()

	s.logger.Info("pipe saved", "name", q.Name)
	return nil
}

// Delete removes a named query from the store.
func (s *Store) Delete(ctx context.Context, name string) error {
	if err := s.kv.Delete(ctx, name); err != nil {
		return fmt.Errorf("delete pipe: %w", err)
	}

	s.mu.Lock()
	delete(s.cached, name)
	s.mu.Unlock()

	s.logger.Info("pipe deleted", "name", name)
	return nil
}

// BindParams replaces {{param}} placeholders in a NamedQuery's SQL with supplied values.
// Returns the bound SQL and positional parameters.
func BindParams(q *NamedQuery, supplied map[string]any) (string, []any, error) {
	sql := q.SQL
	var params []any
	for _, p := range q.Parameters {
		val, ok := supplied[p.Name]
		if !ok {
			if p.Required {
				return "", nil, fmt.Errorf("missing required parameter: %s", p.Name)
			}
			val = p.Default
		}
		placeholder := "{{" + p.Name + "}}"
		count := strings.Count(sql, placeholder)
		if count > 0 {
			sql = strings.ReplaceAll(sql, placeholder, "?")
			for i := 0; i < count; i++ {
				params = append(params, val)
			}
		}
	}
	return sql, params, nil
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

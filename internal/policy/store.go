package policy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
	"gopkg.in/yaml.v3"
)

const (
	kvBucket = "WAVEHOUSE_POLICY"
	kvKey    = "current"
)

// Store manages policy persistence via NATS KV with optional file bootstrap.
type Store struct {
	kv     jetstream.KeyValue
	logger *slog.Logger
	mu     sync.RWMutex
	cached *Policy
}

// NewStore creates a policy store backed by NATS KV.
// If bootstrapPath is non-empty and the file exists, its contents seed the KV store.
func NewStore(ctx context.Context, js jetstream.JetStream, bootstrapPath string, logger *slog.Logger) (*Store, error) {
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  kvBucket,
		History: 5,
	})
	if err != nil {
		return nil, fmt.Errorf("create policy kv bucket: %w", err)
	}

	s := &Store{kv: kv, logger: logger}

	// Bootstrap from file if KV is empty.
	_, err = kv.Get(ctx, kvKey)
	if err != nil {
		if bootstrapPath != "" {
			if p, loadErr := loadPolicyFile(bootstrapPath); loadErr == nil {
				if putErr := s.Put(ctx, p); putErr != nil {
					logger.Warn("policy bootstrap write failed", "error", putErr)
				} else {
					logger.Info("bootstrapped policy from file", "path", bootstrapPath)
				}
			} else if !os.IsNotExist(loadErr) {
				logger.Warn("policy file load failed", "path", bootstrapPath, "error", loadErr)
			}
		}
	}

	// Load current policy into cache.
	if p, err := s.load(ctx); err == nil {
		s.mu.Lock()
		s.cached = p
		s.mu.Unlock()
		s.warnIfDefaultRoleGrantsAdmin(p)
	}

	return s, nil
}

// Get returns the current cached policy. May be nil if no policy is set.
func (s *Store) Get() *Policy {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cached
}

// Put saves a policy to the NATS KV store and updates the local cache.
func (s *Store) Put(ctx context.Context, p *Policy) error {
	if err := Validate(p); err != nil {
		return fmt.Errorf("invalid policy: %w", err)
	}

	data, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshal policy: %w", err)
	}

	if _, err := s.kv.Put(ctx, kvKey, data); err != nil {
		return fmt.Errorf("put policy to kv: %w", err)
	}

	s.mu.Lock()
	s.cached = p
	s.mu.Unlock()

	s.logger.Info("policy updated")
	s.warnIfDefaultRoleGrantsAdmin(p)
	return nil
}

// warnIfDefaultRoleGrantsAdmin logs a loud warning when the policy grants admin
// to every roleless request via default_role == admin_role. The configuration
// is permitted (handy for local/dev — no token needed to reach admin surfaces)
// but unsafe in production, so every node that adopts such a policy says so. It
// runs at each point a policy is taken into the cache: Put, startup load, and a
// Watch update from a peer node.
func (s *Store) warnIfDefaultRoleGrantsAdmin(p *Policy) {
	if DefaultRoleGrantsAdmin(p) {
		s.logger.Warn("default_role equals admin_role: every unauthenticated/roleless request is granted full admin access, including /v1/admin/* — intended for local/dev only, do NOT use in production",
			"default_role", p.DefaultRole)
	}
}

// Watch subscribes to policy changes in the NATS KV store so all nodes
// stay in sync. Blocks until ctx is cancelled.
func (s *Store) Watch(ctx context.Context) {
	watcher, err := s.kv.Watch(ctx, kvKey)
	if err != nil {
		s.logger.Error("policy watch failed", "error", err)
		return
	}
	defer func() { _ = watcher.Stop() }()

	for {
		select {
		case <-ctx.Done():
			return
		case entry := <-watcher.Updates():
			if entry == nil {
				continue
			}
			if entry.Operation() == jetstream.KeyValueDelete || entry.Operation() == jetstream.KeyValuePurge {
				s.handleDelete()
				continue
			}
			var p Policy
			if err := json.Unmarshal(entry.Value(), &p); err != nil {
				s.logger.Error("policy watch unmarshal", "error", err)
				continue
			}
			s.mu.Lock()
			s.cached = &p
			s.mu.Unlock()
			s.logger.Info("policy updated via watch", "revision", entry.Revision())
			s.warnIfDefaultRoleGrantsAdmin(&p)
		}
	}
}

// handleDelete applies a KV delete/purge by clearing the cache. With no policy,
// Evaluate and IsAdmin fail fully closed — nobody passes, not even the admin
// role — so deleting the policy denies all traffic until a new one is written
// (bootstrap from the policy file). No fail-closed retention is needed: the
// deny-by-default is structural, not a runtime flag.
func (s *Store) handleDelete() {
	s.mu.Lock()
	s.cached = nil
	s.mu.Unlock()
	s.logger.Warn("policy deleted from KV; cache cleared — all traffic (including the admin role) is now denied until a new policy is written")
}

// load reads the current policy from NATS KV.
func (s *Store) load(ctx context.Context) (*Policy, error) {
	entry, err := s.kv.Get(ctx, kvKey)
	if err != nil {
		return nil, err
	}
	var p Policy
	if err := json.Unmarshal(entry.Value(), &p); err != nil {
		return nil, fmt.Errorf("unmarshal policy: %w", err)
	}
	return &p, nil
}

// NewMemoryStore creates an in-memory policy store pre-loaded with the given
// policy. Intended for testing — no NATS connection required.
func NewMemoryStore(p *Policy) *Store {
	return &Store{
		cached: p,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// loadPolicyFile reads a policy from a YAML or JSON file.
func loadPolicyFile(path string) (*Policy, error) {
	// #nosec G304 -- path is operator-configured via config.yaml, not request input.
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var p Policy
	if strings.HasSuffix(path, ".json") {
		if err := json.Unmarshal(data, &p); err != nil {
			return nil, fmt.Errorf("parse policy json: %w", err)
		}
	} else {
		if err := yaml.Unmarshal(data, &p); err != nil {
			return nil, fmt.Errorf("parse policy yaml: %w", err)
		}
	}
	return &p, nil
}

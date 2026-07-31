package policy

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/Wave-RF/WaveHouse/internal/controldb"
	"gopkg.in/yaml.v3"
)

// Store manages policy persistence in the control-plane database with
// optional file bootstrap. The Policy document is decomposed into relational
// rows on Put (roles, policy_globals, table_policies, policy_predicates — one
// row per grant, one row per predicate condition) and recomposed on load, so
// the schema's constraints — operator whitelist, check-operator restriction,
// role foreign keys — hold for every write. The in-memory cache is the read
// path; no request touches SQLite.
type Store struct {
	db     *sql.DB
	logger *slog.Logger
	mu     sync.RWMutex
	cached *Policy
}

// NewStore creates a policy store backed by the control-plane database.
//
// Bootstrap semantics:
//   - If the database already holds a policy, it wins — the file is a seed,
//     not the source of truth, so subsequent runs ignore bootstrapPath
//     entirely. Runtime updates flow in via Put.
//   - If the database is empty and bootstrapPath is set, the file MUST exist,
//     parse, validate, and persist; any failure is fatal so a misconfigured
//     deployment refuses to start instead of silently running fail-closed
//     (every request denied, including the admin role) until an operator
//     notices.
//   - If the database is empty and bootstrapPath is "", the store starts with
//     no policy and the operator must seed via Put — every request fails
//     closed in the meantime, which we log loudly.
func NewStore(ctx context.Context, db *sql.DB, bootstrapPath string, logger *slog.Logger) (*Store, error) {
	s := &Store{db: db, logger: logger}

	switch p, err := s.load(ctx); {
	case err == nil:
		s.mu.Lock()
		s.cached = p
		s.mu.Unlock()
		s.warnIfDefaultRoleGrantsAdmin(p)
		return s, nil
	case !errors.Is(err, errNoPolicy):
		return nil, fmt.Errorf("read policy from control db: %w", err)
	}

	if bootstrapPath == "" {
		logger.Warn("control db holds no policy and no bootstrap file is configured — every request will be denied (fail-closed, including the admin role) until a policy is PUT via the API")
		return s, nil
	}

	p, err := loadPolicyFile(bootstrapPath)
	if err != nil {
		return nil, fmt.Errorf("load policy bootstrap file %q: %w", bootstrapPath, err)
	}
	// Put validates, persists, caches, and warns about default_role==admin.
	if err := s.Put(ctx, p); err != nil {
		return nil, fmt.Errorf("bootstrap policy from %q: %w", bootstrapPath, err)
	}
	logger.Info("bootstrapped policy from file", "path", bootstrapPath)
	return s, nil
}

// Get returns the current cached policy. May be nil if no policy is set.
func (s *Store) Get() *Policy {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cached
}

// Put validates the policy, replaces the stored one in a single transaction
// (grants and predicates are deleted and reinserted; roles are upserted and
// never garbage-collected), and updates the local cache.
func (s *Store) Put(ctx context.Context, p *Policy) error {
	if err := Validate(p); err != nil {
		return fmt.Errorf("invalid policy: %w", err)
	}

	if err := s.persist(ctx, p); err != nil {
		return err
	}

	s.mu.Lock()
	s.cached = p
	s.mu.Unlock()

	s.logger.Info("policy updated")
	s.warnIfDefaultRoleGrantsAdmin(p)
	return nil
}

// persist writes the decomposed policy inside one transaction: it fully
// replaces the previous policy or leaves it untouched — there is no
// observable half-applied state.
func (s *Store) persist(ctx context.Context, p *Policy) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin policy write: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Deleting the grants cascades their predicates.
	if _, err := tx.ExecContext(ctx, `DELETE FROM table_policies`); err != nil {
		return fmt.Errorf("clear table_policies: %w", err)
	}

	for tableName, tp := range p.Tables {
		for _, op := range []struct {
			name  string
			perms map[string]RolePermissions
		}{{"select", tp.Select}, {"insert", tp.Insert}} {
			for roleName, perms := range op.perms {
				if err := insertGrant(ctx, tx, tableName, roleName, op.name, perms); err != nil {
					return err
				}
			}
		}
	}

	defaultRoleID, err := roleIDOrNull(ctx, tx, p.DefaultRole)
	if err != nil {
		return err
	}
	adminRoleID, err := roleIDOrNull(ctx, tx, p.AdminRole)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO policy_globals (id, default_role_id, admin_role_id)
		VALUES (1, ?, ?)
		ON CONFLICT (id) DO UPDATE SET default_role_id = excluded.default_role_id, admin_role_id = excluded.admin_role_id`,
		defaultRoleID, adminRoleID); err != nil {
		return fmt.Errorf("write policy_globals: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit policy write: %w", err)
	}
	return nil
}

// insertGrant writes one table_policies row and its predicate rows.
func insertGrant(ctx context.Context, tx *sql.Tx, tableName, roleName, operation string, perms RolePermissions) error {
	roleID, err := controldb.UpsertRole(ctx, tx, roleName)
	if err != nil {
		return err
	}

	allowCols, err := jsonListOrNull(perms.AllowColumns)
	if err != nil {
		return err
	}
	denyCols, err := jsonListOrNull(perms.DenyColumns)
	if err != nil {
		return err
	}
	allowedAggs, err := jsonListOrNull(perms.AllowedAggregations)
	if err != nil {
		return err
	}
	deniedAggs, err := jsonListOrNull(perms.DeniedAggregations)
	if err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO table_policies
		(table_name, role_id, operation, allow_columns, deny_columns,
		 allowed_aggregations, denied_aggregations,
		 max_rows, max_execution_time_ms, max_rows_to_read, max_memory_usage_bytes)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		tableName, roleID, operation, allowCols, denyCols, allowedAggs, deniedAggs,
		perms.MaxRows, int64(perms.MaxExecutionTime), perms.MaxRowsToRead, int64(perms.MaxMemoryUsage),
	); err != nil {
		return fmt.Errorf("insert grant (%s, %s, %s): %w", tableName, roleName, operation, err)
	}

	for _, pred := range []struct {
		kind    string
		filters map[string]Filter
	}{{"filter", perms.Filter}, {"check", perms.Check}} {
		for columnName, f := range pred.filters {
			for operator, value := range filterOperators(f) {
				if _, err := tx.ExecContext(ctx, `INSERT INTO policy_predicates
					(table_name, role_id, operation, kind, column_name, operator, value)
					VALUES (?, ?, ?, ?, ?, ?, ?)`,
					tableName, roleID, operation, pred.kind, columnName, operator, value,
				); err != nil {
					return fmt.Errorf("insert %s predicate (%s, %s, %s, %s %s): %w",
						pred.kind, tableName, roleName, operation, columnName, operator, err)
				}
			}
		}
	}
	return nil
}

// errNoPolicy marks "the database holds no policy" (no policy_globals row) —
// the fail-closed empty state, distinct from a read error.
var errNoPolicy = errors.New("no policy stored")

// load recomposes the Policy document from its relational rows.
func (s *Store) load(ctx context.Context) (*Policy, error) {
	var defaultRole, adminRole sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT dr.name, ar.name
		FROM policy_globals g
		LEFT JOIN roles dr ON dr.id = g.default_role_id
		LEFT JOIN roles ar ON ar.id = g.admin_role_id
		WHERE g.id = 1`).Scan(&defaultRole, &adminRole)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, errNoPolicy
	case err != nil:
		return nil, fmt.Errorf("read policy_globals: %w", err)
	}

	p := &Policy{
		DefaultRole: defaultRole.String,
		AdminRole:   adminRole.String,
		Tables:      map[string]TablePolicy{},
	}

	if err := s.loadGrants(ctx, p); err != nil {
		return nil, err
	}
	if err := s.loadPredicates(ctx, p); err != nil {
		return nil, err
	}
	return p, nil
}

func (s *Store) loadGrants(ctx context.Context, p *Policy) error {
	rows, err := s.db.QueryContext(ctx, `SELECT tp.table_name, r.name, tp.operation,
			tp.allow_columns, tp.deny_columns, tp.allowed_aggregations, tp.denied_aggregations,
			tp.max_rows, tp.max_execution_time_ms, tp.max_rows_to_read, tp.max_memory_usage_bytes
		FROM table_policies tp JOIN roles r ON r.id = tp.role_id`)
	if err != nil {
		return fmt.Errorf("read table_policies: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			tableName, roleName, operation                    string
			allowCols, denyCols, allowedAggs, deniedAggs      sql.NullString
			maxRows                                           int
			maxExecutionTimeMs, maxRowsToRead, maxMemoryUsage int64
		)
		if err := rows.Scan(&tableName, &roleName, &operation,
			&allowCols, &denyCols, &allowedAggs, &deniedAggs,
			&maxRows, &maxExecutionTimeMs, &maxRowsToRead, &maxMemoryUsage); err != nil {
			return fmt.Errorf("scan grant: %w", err)
		}
		perms := RolePermissions{
			MaxRows:          maxRows,
			MaxExecutionTime: Millis(maxExecutionTimeMs),
			MaxRowsToRead:    maxRowsToRead,
			MaxMemoryUsage:   ByteSize(maxMemoryUsage),
		}
		if perms.AllowColumns, err = listFromJSON(allowCols); err != nil {
			return err
		}
		if perms.DenyColumns, err = listFromJSON(denyCols); err != nil {
			return err
		}
		if perms.AllowedAggregations, err = listFromJSON(allowedAggs); err != nil {
			return err
		}
		if perms.DeniedAggregations, err = listFromJSON(deniedAggs); err != nil {
			return err
		}
		setGrant(p, tableName, operation, roleName, perms)
	}
	return rows.Err()
}

func (s *Store) loadPredicates(ctx context.Context, p *Policy) error {
	rows, err := s.db.QueryContext(ctx, `SELECT pp.table_name, r.name, pp.operation,
			pp.kind, pp.column_name, pp.operator, pp.value
		FROM policy_predicates pp JOIN roles r ON r.id = pp.role_id`)
	if err != nil {
		return fmt.Errorf("read policy_predicates: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var tableName, roleName, operation, kind, columnName, operator, value string
		if err := rows.Scan(&tableName, &roleName, &operation, &kind, &columnName, &operator, &value); err != nil {
			return fmt.Errorf("scan predicate: %w", err)
		}
		perms, ok := grantOf(p, tableName, operation, roleName)
		if !ok {
			// FK guarantees the grant exists; a miss means the two reads
			// disagree, which the single-connection store cannot produce.
			return fmt.Errorf("predicate without grant: (%s, %s, %s)", tableName, roleName, operation)
		}
		filters := perms.Filter
		if kind == "check" {
			filters = perms.Check
		}
		if filters == nil {
			filters = map[string]Filter{}
		}
		f := filters[columnName]
		if err := setFilterOperator(&f, operator, value); err != nil {
			return err
		}
		filters[columnName] = f
		if kind == "check" {
			perms.Check = filters
		} else {
			perms.Filter = filters
		}
		setGrant(p, tableName, operation, roleName, perms)
	}
	return rows.Err()
}

// setGrant stores perms at p.Tables[table].(Select|Insert)[role], creating
// maps as needed (maps stay nil until a row needs them, matching omitempty).
func setGrant(p *Policy, tableName, operation, roleName string, perms RolePermissions) {
	tp := p.Tables[tableName]
	switch operation {
	case "select":
		if tp.Select == nil {
			tp.Select = map[string]RolePermissions{}
		}
		tp.Select[roleName] = perms
	case "insert":
		if tp.Insert == nil {
			tp.Insert = map[string]RolePermissions{}
		}
		tp.Insert[roleName] = perms
	}
	p.Tables[tableName] = tp
}

// grantOf fetches the perms a predicate row attaches to.
func grantOf(p *Policy, tableName, operation, roleName string) (RolePermissions, bool) {
	tp, ok := p.Tables[tableName]
	if !ok {
		return RolePermissions{}, false
	}
	var perms RolePermissions
	if operation == "select" {
		perms, ok = tp.Select[roleName]
	} else {
		perms, ok = tp.Insert[roleName]
	}
	return perms, ok
}

// filterOperators flattens a Filter into (operator, value) pairs — one
// predicate row each.
func filterOperators(f Filter) map[string]string {
	out := map[string]string{}
	for op, v := range map[string]*string{"_eq": f.Eq, "_neq": f.Neq, "_gt": f.Gt, "_lt": f.Lt, "_in": f.In} {
		if v != nil {
			out[op] = *v
		}
	}
	return out
}

// setFilterOperator is filterOperators' inverse: it sets one operator field.
func setFilterOperator(f *Filter, operator, value string) error {
	v := value
	switch operator {
	case "_eq":
		f.Eq = &v
	case "_neq":
		f.Neq = &v
	case "_gt":
		f.Gt = &v
	case "_lt":
		f.Lt = &v
	case "_in":
		f.In = &v
	default:
		return fmt.Errorf("unknown predicate operator %q", operator)
	}
	return nil
}

// roleIDOrNull upserts name and returns its id, or SQL NULL for the empty
// name (an unset default/admin role).
func roleIDOrNull(ctx context.Context, tx *sql.Tx, name string) (any, error) {
	if name == "" {
		return nil, nil //nolint:nilnil // nil any IS the value: SQL NULL
	}
	id, err := controldb.UpsertRole(ctx, tx, name)
	if err != nil {
		return nil, err
	}
	return id, nil
}

// jsonListOrNull renders a []string as a JSON array cell, preserving the
// nil/empty distinction: nil (unrestricted) stores NULL, empty (restricted to
// nothing) stores '[]'.
func jsonListOrNull(list []string) (any, error) {
	if list == nil {
		return nil, nil //nolint:nilnil // nil any IS the value: SQL NULL
	}
	data, err := json.Marshal(list)
	if err != nil {
		return nil, fmt.Errorf("marshal column list: %w", err)
	}
	return string(data), nil
}

// listFromJSON is jsonListOrNull's inverse.
func listFromJSON(cell sql.NullString) ([]string, error) {
	if !cell.Valid {
		return nil, nil
	}
	var list []string
	if err := json.Unmarshal([]byte(cell.String), &list); err != nil {
		return nil, fmt.Errorf("unmarshal column list: %w", err)
	}
	if list == nil {
		list = []string{}
	}
	return list, nil
}

// warnIfDefaultRoleGrantsAdmin logs a loud warning when the policy grants admin
// to every roleless request via default_role == admin_role. The configuration
// is permitted (handy for local/dev — no token needed to reach admin surfaces)
// but unsafe in production, so every node that adopts such a policy says so.
func (s *Store) warnIfDefaultRoleGrantsAdmin(p *Policy) {
	if DefaultRoleGrantsAdmin(p) {
		s.logger.Warn("default_role equals admin_role: every unauthenticated/roleless request is granted full admin access, including /v1/admin/* — intended for local/dev only, do NOT use in production",
			"default_role", p.DefaultRole)
	}
}

// NewMemoryStore creates a policy store over an in-memory control database,
// pre-loaded with the given policy. Intended for testing: same engine, same
// constraints, same code paths as production — only the storage lives in RAM.
// The seed policy populates the cache directly (a later Put replaces the
// stored rows wholesale, so nothing needs pre-persisting).
func NewMemoryStore(p *Policy) *Store {
	return &Store{
		db:     controldb.MustOpenMemory(),
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

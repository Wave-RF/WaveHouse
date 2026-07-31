-- Control-plane schema: policy, pipes, and runtime settings move from NATS KV
-- into one SQLite database so integrity rules live in the storage layer
-- (CHECK constraints, foreign keys) instead of only in Go validation.
--
-- Conventions:
--   * STRICT tables — a column stores only its declared type.
--   * uuids and timestamps are TEXT (SQLite has no native types for either);
--     timestamps are RFC 3339 UTC.
--   * Delete rules: parts cascade ("owned by"), dependencies refuse
--     ("points at"). RESTRICT is spelled explicitly where the pointing row can
--     never die in the same statement as its target; edges that can share a
--     cascade keep the default NO ACTION (checked at end of statement) so a
--     whole-entity delete doesn't trip over its own ordering.
--   * Where the future schema catalog would supply table/column ids, rows key
--     on ClickHouse table/column NAMES: WaveHouse is Bring-Your-Own-Schema
--     today, so ClickHouse remains the source of truth for what exists.

-- Schema catalog: ClickHouse tables and their columns. DORMANT for now —
-- nothing writes or references these yet. They become live (and the name-keyed
-- policy/settings rows below graduate to id-based foreign keys) when WaveHouse
-- takes ownership of ClickHouse DDL; until then ClickHouse is user-managed
-- (Bring-Your-Own-Schema), so a populated catalog could only mirror it, and
-- constraints against a mirror cannot protect the real schema.
CREATE TABLE tables (
    id   TEXT NOT NULL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE CHECK (name <> '')
) STRICT;

CREATE TABLE columns (
    id           TEXT NOT NULL PRIMARY KEY,
    table_id     TEXT NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
    name         TEXT NOT NULL CHECK (name <> ''),
    type         TEXT NOT NULL,
    default_expr TEXT,
    UNIQUE (table_id, name),
    UNIQUE (table_id, id) -- target for future composite foreign keys
) STRICT;

-- Roles referenced by policy grants, pipe allowlists, and the globals row.
-- Rows are upserted on demand when a policy or pipe references a new name and
-- are never garbage-collected; an unreferenced role row is harmless.
CREATE TABLE roles (
    id   TEXT NOT NULL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE CHECK (name <> '')
) STRICT;

-- Singleton: deployment-wide policy roles. NULL = unset (no public access /
-- the compiled "admin" default). Row presence marks "a policy exists" —
-- deleting it is how the store distinguishes fail-closed no-policy state.
CREATE TABLE policy_globals (
    id              INTEGER NOT NULL PRIMARY KEY CHECK (id = 1),
    default_role_id TEXT REFERENCES roles(id) ON DELETE RESTRICT,
    admin_role_id   TEXT REFERENCES roles(id) ON DELETE RESTRICT
) STRICT;

-- One row = one grant: (table, role, operation) plus its fixed-shape knobs.
-- Column lists are JSON arrays (NULL = unrestricted); limits use the Go zero
-- value for "unset", mirroring policy.RolePermissions.
CREATE TABLE table_policies (
    table_name             TEXT NOT NULL CHECK (table_name <> ''),
    role_id                TEXT NOT NULL REFERENCES roles(id) ON DELETE RESTRICT,
    operation              TEXT NOT NULL CHECK (operation IN ('select', 'insert')),
    allow_columns          TEXT,
    deny_columns           TEXT,
    allowed_aggregations   TEXT,
    denied_aggregations    TEXT,
    max_rows               INTEGER NOT NULL DEFAULT 0 CHECK (max_rows >= 0),
    max_execution_time_ms  INTEGER NOT NULL DEFAULT 0 CHECK (max_execution_time_ms >= 0),
    max_rows_to_read       INTEGER NOT NULL DEFAULT 0 CHECK (max_rows_to_read >= 0),
    max_memory_usage_bytes INTEGER NOT NULL DEFAULT 0 CHECK (max_memory_usage_bytes >= 0),
    PRIMARY KEY (table_name, role_id, operation)
) STRICT;

-- One row = one predicate condition on a grant. kind 'filter' renders into a
-- SELECT WHERE clause; kind 'check' validates ingested rows. The operator set
-- and the check-operator restriction (#224: checks are _eq/_in only) are
-- storage-enforced. operator is part of the key so one column can carry a
-- range (_gt + _lt as two rows).
CREATE TABLE policy_predicates (
    table_name  TEXT NOT NULL,
    role_id     TEXT NOT NULL,
    operation   TEXT NOT NULL,
    kind        TEXT NOT NULL CHECK (kind IN ('filter', 'check')),
    column_name TEXT NOT NULL CHECK (column_name <> ''),
    operator    TEXT NOT NULL CHECK (operator IN ('_eq', '_neq', '_gt', '_lt', '_in')),
    value       TEXT NOT NULL,
    PRIMARY KEY (table_name, role_id, operation, kind, column_name, operator),
    CHECK (NOT (kind = 'check' AND operator IN ('_neq', '_gt', '_lt'))),
    FOREIGN KEY (table_name, role_id, operation)
        REFERENCES table_policies (table_name, role_id, operation) ON DELETE CASCADE
) STRICT;

-- Named queries. The surrogate id exists so a pipe can be renamed without
-- rewriting its child rows; name stays unique because it is the URL segment.
CREATE TABLE pipes (
    id          TEXT NOT NULL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE CHECK (name <> ''),
    sql         TEXT NOT NULL CHECK (sql <> ''),
    description TEXT NOT NULL DEFAULT ''
) STRICT;

-- One row = one declared parameter. position preserves declaration order;
-- default_val is a JSON literal (42 vs "42" vs [1,2]) so the original type
-- drives SQL rendering; NULL = no default.
CREATE TABLE pipe_params (
    pipe_id     TEXT NOT NULL REFERENCES pipes(id) ON DELETE CASCADE,
    param_name  TEXT NOT NULL CHECK (param_name <> ''),
    position    INTEGER NOT NULL,
    type        TEXT NOT NULL DEFAULT '',
    required    INTEGER NOT NULL DEFAULT 0 CHECK (required IN (0, 1)),
    default_val TEXT,
    PRIMARY KEY (pipe_id, param_name)
) STRICT;

-- One row = one role allowed to execute a pipe. Zero rows = admin-only
-- (fails closed).
CREATE TABLE pipe_roles (
    pipe_id  TEXT NOT NULL REFERENCES pipes(id) ON DELETE CASCADE,
    role_id  TEXT NOT NULL REFERENCES roles(id) ON DELETE RESTRICT,
    position INTEGER NOT NULL,
    PRIMARY KEY (pipe_id, role_id)
) STRICT;

-- Runtime settings: one table per section; row absent = compiled defaults,
-- row present = stored override. revision is the optimistic-concurrency token
-- carried by If-Match. Deployment-global today; per-table dedupe waits on the
-- schema catalog (#222).
CREATE TABLE dedupe_settings (
    id         INTEGER NOT NULL PRIMARY KEY CHECK (id = 1),
    id_field   TEXT NOT NULL,
    require_id INTEGER NOT NULL DEFAULT 0 CHECK (require_id IN (0, 1)),
    revision   INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    CHECK (NOT (require_id = 1 AND id_field = ''))
) STRICT;


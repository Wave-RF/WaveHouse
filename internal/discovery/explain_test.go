package discovery

import (
	"reflect"
	"testing"
)

// parseExplainTables is the brittle string-handling half of ResolveTables; the
// sample lines below are real EXPLAIN QUERY TREE output shapes (verified against
// ClickHouse 26.6). The ResolveTables round-trip itself is covered by the
// integration test against a live server.
func TestParseExplainTables(t *testing.T) {
	tests := []struct {
		name       string
		lines      []string
		db         string
		wantTables []string
		wantDicts  []string
	}{
		{
			name: "simple join, both local",
			lines: []string{
				"    TABLE id: 3, alias: __table1, table_name: default.base",
				"    TABLE id: 5, alias: __table2, table_name: default.other",
			},
			db:         "default",
			wantTables: []string{"base", "other"},
		},
		{
			name:       "count() keeps the table (the whole point)",
			lines:      []string{"  TABLE id: 3, alias: __table1, table_name: default.events"},
			db:         "default",
			wantTables: []string{"events"},
		},
		{
			name: "weird name with dots+spaces: split on FIRST dot only",
			lines: []string{
				"    TABLE id: 3, alias: __table1, table_name: default.weird.name 2024",
			},
			db:         "default",
			wantTables: []string{"weird.name 2024"},
		},
		{
			name: "foreign database is dropped (unsupported, documented)",
			lines: []string{
				"    TABLE id: 3, alias: __table1, table_name: otherdb.events",
				"    TABLE id: 5, alias: __table2, table_name: default.local",
			},
			db:         "default",
			wantTables: []string{"local"},
		},
		{
			name: "dedup repeated table (self-join)",
			lines: []string{
				"    TABLE id: 3, table_name: default.t",
				"    TABLE id: 9, table_name: default.t",
			},
			db:         "default",
			wantTables: []string{"t"},
		},
		{
			name:  "table function: no table_name line → nothing",
			lines: []string{"  QUERY id: 0", "    JOIN TREE", "      TABLE_FUNCTION numbers"},
			db:    "default",
		},
		{
			name:  "no tables (SELECT 1)",
			lines: []string{"QUERY id: 0", "  PROJECTION COLUMNS", "    1 UInt8"},
			db:    "default",
		},
		{
			name: "joinGet: Join-engine table tracked from the first constant",
			lines: []string{
				"      FUNCTION id: 4, function_name: joinGet, function_type: ordinary, result_type: String",
				"            CONSTANT id: 6, constant_value: 'default.jt', constant_value_type: String",
				"            CONSTANT id: 7, constant_value: 'v', constant_value_type: String",
			},
			db:         "default",
			wantTables: []string{"jt"},
		},
		{
			name: "joinGet alongside a FROM table: both tracked",
			lines: []string{
				"    TABLE id: 3, alias: __table1, table_name: default.users",
				"      FUNCTION id: 4, function_name: joinGet, function_type: ordinary, result_type: String",
				"            CONSTANT id: 6, constant_value: 'default.jt', constant_value_type: String",
			},
			db:         "default",
			wantTables: []string{"jt", "users"},
		},
		{
			name: "joinGet bare (unqualified) name → configured db",
			lines: []string{
				"      FUNCTION id: 4, function_name: joinGet, function_type: ordinary, result_type: String",
				"            CONSTANT id: 6, constant_value: 'jt', constant_value_type: String",
			},
			db:         "default",
			wantTables: []string{"jt"},
		},
		{
			name: "joinGet cross-db dropped",
			lines: []string{
				"      FUNCTION id: 4, function_name: joinGet, function_type: ordinary, result_type: String",
				"            CONSTANT id: 6, constant_value: 'otherdb.jt', constant_value_type: String",
			},
			db: "default",
		},
		{
			name: "joinGet with a non-string first constant disarms (no false capture)",
			lines: []string{
				"      FUNCTION id: 4, function_name: joinGet, function_type: ordinary, result_type: String",
				"            CONSTANT id: 6, constant_value: UInt64_1, constant_value_type: UInt64",
				"            CONSTANT id: 7, constant_value: 'v', constant_value_type: String",
			},
			db: "default",
		},
		{
			name: "dictGet: dictionary captured; the projection's stray '' before it is ignored",
			lines: []string{
				"      CONSTANT id: 2, constant_value: '', constant_value_type: String",
				"          FUNCTION id: 3, function_name: dictGet, function_type: ordinary, result_type: String",
				"                CONSTANT id: 5, constant_value: 'default.mydict', constant_value_type: String",
				"                CONSTANT id: 6, constant_value: 'val', constant_value_type: String",
			},
			db:        "default",
			wantDicts: []string{"mydict"},
		},
		{
			name: "dictGet cross-db dropped",
			lines: []string{
				"          FUNCTION id: 3, function_name: dictGet, function_type: ordinary, result_type: String",
				"                CONSTANT id: 5, constant_value: 'otherdb.mydict', constant_value_type: String",
			},
			db: "default",
		},
		{
			name: "joinGet and dictGet together",
			lines: []string{
				"    TABLE id: 3, table_name: default.users",
				"      FUNCTION id: 4, function_name: joinGet, function_type: ordinary, result_type: String",
				"            CONSTANT id: 6, constant_value: 'default.jt', constant_value_type: String",
				"      FUNCTION id: 8, function_name: dictGetOrDefault, function_type: ordinary, result_type: String",
				"            CONSTANT id: 9, constant_value: 'default.mydict', constant_value_type: String",
			},
			db:         "default",
			wantTables: []string{"jt", "users"},
			wantDicts:  []string{"mydict"},
		},
		{
			name:       "FINAL: trailing ', final: 1' must not leak into the table name",
			lines:      []string{"  TABLE id: 3, alias: __table1, table_name: default.rmt, final: 1"},
			db:         "default",
			wantTables: []string{"rmt"},
		},
		{
			name:       "SAMPLE: trailing ', final: 0, sample_size: …' stripped",
			lines:      []string{"  TABLE id: 3, table_name: default.smp, final: 0, sample_size: 5 / 10"},
			db:         "default",
			wantTables: []string{"smp"},
		},
		{
			name: "string literal containing 'table_name:' is not mistaken for a table",
			lines: []string{
				"      CONSTANT id: 2, constant_value: 'table_name: default.injected', constant_value_type: String",
				"    TABLE id: 3, alias: __table1, table_name: default.real",
			},
			db:         "default",
			wantTables: []string{"real"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTables, gotDicts, _ := parseExplainTables(tt.lines, tt.db)
			if !reflect.DeepEqual(gotTables, tt.wantTables) {
				t.Errorf("tables = %v, want %v", gotTables, tt.wantTables)
			}
			if !reflect.DeepEqual(gotDicts, tt.wantDicts) {
				t.Errorf("dicts = %v, want %v", gotDicts, tt.wantDicts)
			}
		})
	}
}

// TestParseExplainTables_DeadBranchPruning covers the parameter-gated UNION case:
// after binding, a dead arm's WHERE folds to "CONSTANT UInt64_0" and its tables are
// dropped, while a live arm (WHERE → 1, a real predicate, or an "x = 0" compare) is
// kept. The fixtures reproduce real EXPLAIN QUERY TREE indentation (ClickHouse 26.6),
// since pruning is structure-aware. The cardinal safety property: pruning fails
// toward KEEPING — only an exact constant-false WHERE child drops a table.
func TestParseExplainTables_DeadBranchPruning(t *testing.T) {
	// arm builds one UNION-arm QUERY: a table under JOIN TREE and a WHERE whose
	// folded child is the given constant ("UInt64_1" live, "UInt64_0" dead) or a raw
	// node line (e.g. a FUNCTION) for a non-folded predicate.
	arm := func(table, whereChild string) []string {
		return []string{
			"          QUERY id: 6, alias: __table2",
			"            JOIN TREE",
			"              TABLE id: 9, alias: __table3, table_name: default." + table,
			"            WHERE",
			"              " + whereChild,
		}
	}
	wrap := func(arms ...[]string) []string {
		out := []string{
			"QUERY id: 0",
			"  JOIN TREE",
			"    UNION id: 3, alias: __table1, is_subquery: 1, union_mode: UNION_ALL",
			"        LIST id: 5, nodes: 2",
		}
		for _, a := range arms {
			out = append(out, a...)
		}
		return out
	}
	const (
		live = "CONSTANT id: 10, constant_value: UInt64_1, constant_value_type: UInt8"
		dead = "CONSTANT id: 19, constant_value: UInt64_0, constant_value_type: UInt8"
		pred = "FUNCTION id: 4, function_name: greater, function_type: ordinary, result_type: UInt8"
		eq0  = "FUNCTION id: 4, function_name: equals, function_type: ordinary, result_type: UInt8"
	)

	tests := []struct {
		name       string
		lines      []string
		wantTables []string
		wantPruned int
	}{
		{
			name:       "source='web': mobile arm dead, pruned",
			lines:      wrap(arm("web_events", live), arm("mobile_events", dead)),
			wantTables: []string{"web_events"},
			wantPruned: 1,
		},
		{
			name:       "both arms live: nothing pruned",
			lines:      wrap(arm("web_events", live), arm("mobile_events", live)),
			wantTables: []string{"mobile_events", "web_events"},
			wantPruned: 0,
		},
		{
			name:       "real predicate (WHERE col>5) is NOT a dead arm",
			lines:      wrap(arm("web_events", pred), arm("mobile_events", dead)),
			wantTables: []string{"web_events"},
			wantPruned: 1,
		},
		{
			name:       "WHERE x=0 (equals function, not a bare constant) is NOT dead",
			lines:      wrap(arm("web_events", eq0), arm("mobile_events", dead)),
			wantTables: []string{"web_events"},
			wantPruned: 1,
		},
		{
			name: "a table in BOTH a live and a dead arm stays tracked",
			lines: wrap(
				arm("shared", live),
				arm("shared", dead),
				arm("mobile_events", dead),
			),
			wantTables: []string{"shared"},
			wantPruned: 1, // only mobile_events
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTables, _, gotPruned := parseExplainTables(tt.lines, "default")
			if !reflect.DeepEqual(gotTables, tt.wantTables) {
				t.Errorf("tables = %v, want %v", gotTables, tt.wantTables)
			}
			if gotPruned != tt.wantPruned {
				t.Errorf("pruned = %d, want %d", gotPruned, tt.wantPruned)
			}
		})
	}
}

func TestParseDictSourceTable(t *testing.T) {
	tests := []struct {
		name, source, db, want string
		ok                     bool
	}{
		{"clickhouse qualified, same db", "ClickHouse: default.dsrc", "default", "dsrc", true},
		{"clickhouse bare name", "ClickHouse: dsrc", "default", "dsrc", true},
		{"clickhouse other db dropped", "ClickHouse: otherdb.dsrc", "default", "", false},
		{"file source untracked", "File: /var/lib/x.csv", "default", "", false},
		{"executable source untracked", "Executable: ./gen.sh", "default", "", false},
		{"http source untracked", "HTTP: http://x/y", "default", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseDictSourceTable(tt.source, tt.db)
			if got != tt.want || ok != tt.ok {
				t.Errorf("parseDictSourceTable(%q) = (%q, %v), want (%q, %v)", tt.source, got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestConstantString(t *testing.T) {
	tests := []struct {
		name, line, wantVal string
		wantStr, wantFound  bool
	}{
		{"quoted string", "  CONSTANT id: 6, constant_value: 'default.jt', constant_value_type: String", "default.jt", true, true},
		{"empty string", "  CONSTANT id: 2, constant_value: '', constant_value_type: String", "", true, true},
		{"non-string constant", "  CONSTANT id: 7, constant_value: UInt64_0, constant_value_type: UInt8", "", false, true},
		{"not a constant line", "    TABLE id: 3, table_name: default.users", "", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, isStr, found := constantString(tt.line)
			if val != tt.wantVal || isStr != tt.wantStr || found != tt.wantFound {
				t.Errorf("constantString(%q) = (%q,%v,%v), want (%q,%v,%v)", tt.line, val, isStr, found, tt.wantVal, tt.wantStr, tt.wantFound)
			}
		})
	}
}

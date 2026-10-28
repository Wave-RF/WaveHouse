package observability

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureDefault routes the default logger to the returned buffer as JSON.
// The default logger is process-wide, so its callers are not parallel.
func captureDefault(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// TestCrash_LogsThenPanicsAgain: a panic under Crash logs one ERROR record
// with the component, the error, the stack and the caller's attributes, then
// panics again with the same value (#902). Not parallel: it captures the
// default logger.
func TestCrash_LogsThenPanicsAgain(t *testing.T) {
	buf := captureDefault(t)
	var repanicked any
	func() {
		defer func() { repanicked = recover() }()
		func() {
			defer Crash("ingest_worker", slog.String("table", "clicks"))
			panic("boom")
		}()
	}()
	assert.Equal(t, "boom", repanicked, "the panic must go on, so the process exits as before")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 1, buf.String())
	var rec map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &rec))
	assert.Equal(t, "ERROR", rec["level"])
	assert.Equal(t, "panic in background goroutine", rec["msg"])
	assert.Equal(t, "ingest_worker", rec["component"])
	assert.Equal(t, "boom", rec["error"])
	assert.Equal(t, "clicks", rec["table"])
	assert.Contains(t, rec["stack"], "crash_test.go", "the stack names where the panic happened")
}

// TestCrash_NoPanicDoesNothing: without a panic Crash neither logs nor
// panics. Not parallel: it captures the default logger.
func TestCrash_NoPanicDoesNothing(t *testing.T) {
	buf := captureDefault(t)
	assert.NotPanics(t, func() {
		defer Crash("shard_claims")
	})
	assert.Empty(t, buf.String())
}

// TestGoroutinesAreCrashGuarded scans the engine's source for every
// goroutine it starts, a go statement or a WaitGroup.Go, and fails if the
// function is not a literal whose first statement is
// defer observability.Crash(...), so a panic in any of them leaves a
// structured record before the process exits (#902). A named function is
// wrapped in a literal at the call, where the component is named.
func TestGoroutinesAreCrashGuarded(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	checked := 0
	for _, root := range []string{"../../internal", "../../cmd", "../../pkg"} {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			// Test helpers run only under go test.
			if d.IsDir() && (d.Name() == "testutil" || strings.HasSuffix(d.Name(), "test")) {
				return filepath.SkipDir
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			ast.Inspect(file, func(n ast.Node) bool {
				var fn ast.Expr
				switch n := n.(type) {
				case *ast.GoStmt:
					fn = n.Call.Fun
				case *ast.CallExpr:
					sel, ok := n.Fun.(*ast.SelectorExpr)
					switch {
					case ok && sel.Sel.Name == "Go" && len(n.Args) == 1:
						fn = n.Args[0]
					// A JetStream Consume calls its handler on a
					// goroutine of the client's (internal/mq is the only
					// package that imports JetStream).
					case ok && sel.Sel.Name == "Consume" && len(n.Args) > 0 && jetstreamFile(file):
						fn = n.Args[0]
					default:
						return true
					}
				default:
					return true
				}
				checked++
				assert.Truef(t, crashGuarded(fn),
					"%s: a goroutine must be a func literal starting with defer observability.Crash(...)", fset.Position(n.Pos()))
				return true
			})
			return nil
		})
		require.NoError(t, err)
	}
	assert.Positive(t, checked, "the scan found no goroutine, so it is not reading the source")
}

// jetstreamFile reports whether file imports the JetStream client.
func jetstreamFile(file *ast.File) bool {
	for _, imp := range file.Imports {
		if imp.Path.Value == `"github.com/nats-io/nats.go/jetstream"` {
			return true
		}
	}
	return false
}

// crashGuarded reports whether fn is a func literal whose first statement
// defers Crash (observability.Crash outside this package).
func crashGuarded(fn ast.Expr) bool {
	lit, ok := fn.(*ast.FuncLit)
	if !ok || len(lit.Body.List) == 0 {
		return false
	}
	d, ok := lit.Body.List[0].(*ast.DeferStmt)
	if !ok {
		return false
	}
	switch f := d.Call.Fun.(type) {
	case *ast.Ident:
		return f.Name == "Crash"
	case *ast.SelectorExpr:
		pkg, ok := f.X.(*ast.Ident)
		return ok && pkg.Name == "observability" && f.Sel.Name == "Crash"
	}
	return false
}

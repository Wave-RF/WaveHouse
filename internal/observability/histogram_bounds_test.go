package observability

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// defaultBoundsExempt names the seconds histograms that keep the SDK's
// default buckets on purpose. Those bounds (0 to 10,000) fit a stream
// lifetime of up to about 2.8 hours.
var defaultBoundsExempt = map[string]bool{
	"wavehouse_sse_stream_duration_seconds": true,
}

// TestSecondsHistogramsDeclareBuckets scans the engine's source for every
// wavehouse_* histogram in seconds (named _seconds, or with unit "s") and
// fails if one is built without
// metric.WithExplicitBucketBoundaries. The SDK's default buckets are sized
// for milliseconds, so every observation under 5 s would land in one bucket
// (#898).
func TestSecondsHistogramsDeclareBuckets(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	found := map[string]bool{}
	for _, root := range []string{"../../internal", "../../pkg", "../../cmd"} {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			ast.Inspect(file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok || len(call.Args) == 0 {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || (sel.Sel.Name != "Float64Histogram" && sel.Sel.Name != "Int64Histogram") {
					return true
				}
				lit, ok := call.Args[0].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					assert.Failf(t, "histogram name is not a string literal",
						"%s: name the histogram with a literal so this check can read it", fset.Position(call.Pos()))
					return true
				}
				name, err := strconv.Unquote(lit.Value)
				if err != nil || !strings.HasPrefix(name, "wavehouse_") {
					return true
				}
				// The Prometheus exporter appends _seconds to a histogram whose unit is "s".
				if !strings.HasSuffix(name, "_seconds") && !hasSecondsUnit(call.Args[1:]) {
					return true
				}
				found[name] = true
				if defaultBoundsExempt[name] {
					return true
				}
				assert.Truef(t, hasBucketOption(call.Args[1:]),
					"%s (%s) declares no metric.WithExplicitBucketBoundaries", name, fset.Position(call.Pos()))
				return true
			})
			return nil
		})
		if !assert.NoError(t, err) {
			return
		}
	}
	// Guard the scan itself: these must be seen, or it found nothing to check.
	for _, name := range []string{
		"wavehouse_dedupe_dynamodb_request_duration_seconds",
		"wavehouse_ingest_shard_handover_seconds",
		"wavehouse_http_request_duration_seconds",
	} {
		require.Truef(t, found[name], "scan did not find %s", name)
	}
	for name := range defaultBoundsExempt {
		assert.Truef(t, found[name], "exempt histogram %s no longer exists; drop it from defaultBoundsExempt", name)
	}
}

func hasBucketOption(opts []ast.Expr) bool {
	return findOption(opts, "WithExplicitBucketBoundaries") != nil
}

func hasSecondsUnit(opts []ast.Expr) bool {
	call := findOption(opts, "WithUnit")
	if call == nil || len(call.Args) != 1 {
		return false
	}
	lit, ok := call.Args[0].(*ast.BasicLit)
	return ok && lit.Value == `"s"`
}

// findOption returns the first option built by a call to a function named fn.
func findOption(opts []ast.Expr, fn string) *ast.CallExpr {
	for _, opt := range opts {
		call, ok := opt.(*ast.CallExpr)
		if !ok {
			continue
		}
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == fn {
			return call
		}
	}
	return nil
}

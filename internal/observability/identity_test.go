package observability

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/Wave-RF/WaveHouse/internal/testutil/logtest"
)

// testIdentity names the process under test in its telemetry.
var testIdentity = Identity{
	ServiceName: "wavehouse-test",
	Version:     "v1.2.3",
	GitCommit:   "abc123",
	BuildTime:   "2026-10-12T00:00:00Z",
	InstanceID:  "pod-a-1234abcd",
	Roles:       []string{"api", "ingest"},
}

func attrs(res *resource.Resource) map[attribute.Key]string {
	m := map[attribute.Key]string{}
	for _, kv := range res.Attributes() {
		m[kv.Key] = kv.Value.String()
	}
	return m
}

func TestNewResource_NamesTheProcess(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")

	assert.Equal(t, map[attribute.Key]string{
		"service.name":         "wavehouse-test",
		"service.version":      "v1.2.3",
		"service.instance.id":  "pod-a-1234abcd",
		"wavehouse.roles":      "api,ingest",
		"wavehouse.git_commit": "abc123",
		"wavehouse.build_time": "2026-10-12T00:00:00Z",
	}, attrs(newResource(t.Context(), testIdentity)))
}

func TestNewResource_EmptyFieldsAreLeftOut(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")

	assert.Equal(t, map[attribute.Key]string{"service.name": "wavehouse"},
		attrs(newResource(t.Context(), Identity{ServiceName: "wavehouse"})))
}

func TestNewResource_EnvironmentOverridesKeyByKey(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "wavehouse-ingest")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.instance.id=ingest-0,deployment.environment=prod")

	got := attrs(newResource(t.Context(), testIdentity))
	assert.Equal(t, "wavehouse-ingest", got["service.name"], "OTEL_SERVICE_NAME wins")
	assert.Equal(t, "ingest-0", got["service.instance.id"], "OTEL_RESOURCE_ATTRIBUTES wins on a shared key")
	assert.Equal(t, "prod", got["deployment.environment"], "OTEL_RESOURCE_ATTRIBUTES adds keys")
	assert.Equal(t, "v1.2.3", got["service.version"], "a key the environment leaves alone keeps WaveHouse's value")
	assert.Equal(t, "api,ingest", got["wavehouse.roles"])
}

func TestNewResource_MalformedEnvironmentIsNotFatal(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelWarn)
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "broken,team=ops")

	got := attrs(newResource(t.Context(), testIdentity))
	assert.Equal(t, "ops", got["team"], "the entries that could be read apply")
	assert.Equal(t, "pod-a-1234abcd", got["service.instance.id"])
	assert.Contains(t, logs.String(), "OTEL_RESOURCE_ATTRIBUTES is malformed")
}

func TestIdentity_LogAttrs(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []any{
		"version", "v1.2.3",
		"build_time", "2026-10-12T00:00:00Z",
		"git_commit", "abc123",
		"instance_id", "pod-a-1234abcd",
	}, testIdentity.LogAttrs())
}

// TestInitProvider_ExportsBuildInfoAndTargetInfo scrapes the Prometheus
// handler: wavehouse_build_info names the build and roles, and target_info
// carries the resource, the instance included.
func TestInitProvider_ExportsBuildInfoAndTargetInfo(t *testing.T) {
	// t.Setenv keeps this serial, which InitProvider's global state needs.
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
	savedProp := otel.GetTextMapPropagator()
	savedMP := otel.GetMeterProvider()
	t.Cleanup(func() {
		otel.SetTextMapPropagator(savedProp)
		otel.SetMeterProvider(savedMP)
	})

	shutdown, promHandler, err := InitProvider(t.Context(), testIdentity, ProviderConfig{PrometheusEnabled: true})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = shutdown(ctx)
	})
	require.NotNil(t, promHandler)

	rec := httptest.NewRecorder()
	promHandler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	out := string(body)

	assert.Regexp(t, `(?m)^wavehouse_build_info\{[^}]*build_time="2026-10-12T00:00:00Z"[^}]*git_commit="abc123"[^}]*roles="api,ingest"[^}]*version="v1.2.3"[^}]*\} 1$`, out)
	assert.Regexp(t, `(?m)^target_info\{[^}]*service_instance_id="pod-a-1234abcd"[^}]*\} 1$`, out)
	assert.Regexp(t, `(?m)^target_info\{[^}]*wavehouse_roles="api,ingest"[^}]*\} 1$`, out)
}

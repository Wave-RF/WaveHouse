package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wave-RF/WaveHouse/internal/discovery"
	"github.com/Wave-RF/WaveHouse/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchema_List(t *testing.T) {
	t.Parallel()
	reg := discovery.NewSchemaRegistryFromMap([]*discovery.TableSchema{
		{Name: "clicks", Columns: []discovery.Column{{Name: "page", Type: "String"}}},
		{Name: "users", Columns: []discovery.Column{{Name: "name", Type: "String"}}},
	})
	h := NewSchemaHandler(reg)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/schema", nil)
	h.List(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	var tables []*discovery.TableSchema
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &tables))
	assert.Len(t, tables, 2)
}

func TestSchema_Get_Exists(t *testing.T) {
	t.Parallel()
	reg := discovery.NewSchemaRegistryFromMap([]*discovery.TableSchema{
		{Name: "clicks", Columns: []discovery.Column{
			{Name: "page", Type: "String"},
			{Name: "count", Type: "UInt64"},
		}},
	})
	h := NewSchemaHandler(reg)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/schema?table=clicks", nil)

	h.Get(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	var schema discovery.TableSchema
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &schema))
	assert.Equal(t, "clicks", schema.Name)
	assert.Len(t, schema.Columns, 2)
}

func TestSchema_Get_NotFound(t *testing.T) {
	t.Parallel()
	reg := discovery.NewSchemaRegistryFromMap([]*discovery.TableSchema{})
	h := NewSchemaHandler(reg)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/schema?table=nonexistent", nil)

	h.Get(w, r)

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Contains(t, w.Body.String(), "table not found")
	testutil.AssertJSONErrorResponse(t, w)
}

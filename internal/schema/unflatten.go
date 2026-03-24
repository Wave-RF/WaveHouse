package schema

import (
	"sort"
	"strconv"
	"strings"
)

// Unflatten reconstructs a nested JSON-like structure from three typed maps
// produced by Flatten. Dot-notation keys are split into nested maps, and
// consecutive numeric indices (e.g., "tags.0", "tags.1") are reconstructed
// as slices.
func Unflatten(strData map[string]string, numData map[string]float64, boolData map[string]bool) map[string]any {
	root := make(map[string]any)

	for k, v := range strData {
		setNested(root, k, v)
	}
	for k, v := range numData {
		setNested(root, k, v)
	}
	for k, v := range boolData {
		setNested(root, k, v)
	}

	convertArrays(root)
	return root
}

// setNested places a value at the dot-separated key path, creating intermediate
// maps as needed. Numeric path segments are treated as potential array indices
// and stored in maps keyed by the index string; convertArrays collapses them later.
func setNested(root map[string]any, key string, value any) {
	parts := strings.Split(key, ".")
	current := root

	for i, part := range parts {
		if i == len(parts)-1 {
			current[part] = value
			return
		}
		next, ok := current[part]
		if !ok {
			child := make(map[string]any)
			current[part] = child
			current = child
		} else if m, ok := next.(map[string]any); ok {
			current = m
		} else {
			// Conflict: a leaf value already occupies this path segment.
			// Overwrite with a map so deeper keys can be set.
			child := make(map[string]any)
			current[part] = child
			current = child
		}
	}
}

// convertArrays walks the tree and converts any map[string]any whose keys are
// all consecutive integer strings starting from 0 into a []any slice.
func convertArrays(m map[string]any) {
	for k, v := range m {
		if child, ok := v.(map[string]any); ok {
			convertArrays(child)
			if arr, ok := tryConvertToSlice(child); ok {
				m[k] = arr
			}
		}
	}
}

// tryConvertToSlice checks if all keys are consecutive integers starting from 0.
func tryConvertToSlice(m map[string]any) ([]any, bool) {
	if len(m) == 0 {
		return nil, false
	}

	// Check all keys are non-negative integers.
	indices := make([]int, 0, len(m))
	for k := range m {
		idx, err := strconv.Atoi(k)
		if err != nil || idx < 0 {
			return nil, false
		}
		indices = append(indices, idx)
	}

	sort.Ints(indices)

	// Must be consecutive starting from 0.
	for i, idx := range indices {
		if idx != i {
			return nil, false
		}
	}

	arr := make([]any, len(indices))
	for i := range indices {
		arr[i] = m[strconv.Itoa(i)]
	}
	return arr, true
}

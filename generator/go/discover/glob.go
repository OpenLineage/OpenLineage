/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

// Package discover provides schema loading and facet discovery from JSON Schema files.
package discover

import (
	"path/filepath"
	"sort"
)

// ResolveGlobs expands glob patterns and returns a sorted, deduplicated list of absolute paths.
func ResolveGlobs(globs []string) ([]string, error) {
	seen := map[string]struct{}{}
	var out []string

	for _, g := range globs {
		matches, err := filepath.Glob(g)
		if err != nil {
			return nil, err
		}
		for _, m := range matches {
			abs, err := filepath.Abs(m)
			if err != nil {
				return nil, err
			}
			if _, ok := seen[abs]; ok {
				continue
			}
			seen[abs] = struct{}{}
			out = append(out, abs)
		}
	}

	sort.Strings(out)
	return out, nil
}

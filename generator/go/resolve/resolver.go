/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

// Package resolve provides JSON Schema $ref resolution and $defs lookup.
package resolve

import (
	"log"
	"strings"

	"github.com/atombender/go-jsonschema/pkg/schemas"
)

// Resolver resolves JSON Schema $ref pointers to their target schemas.
type Resolver struct {
	defs map[string]*schemas.Type
}

// New creates a Resolver backed by the given definitions map.
func New(defs map[string]*schemas.Type) *Resolver {
	return &Resolver{defs: defs}
}

// Resolve follows $ref chains and returns the final pointed-to schema.
// Returns nil for ignorable base-facet refs, unresolvable refs, or if the
// chain exceeds 32 hops (guards against self-referential / cyclic schemas).
func (r *Resolver) Resolve(t *schemas.Type) *schemas.Type {
	if t == nil {
		return nil
	}

	for depth := 0; t.Ref != ""; depth++ {
		if depth > 32 {
			log.Fatalf("resolve: $ref chain exceeds 32 hops at %q — possible cycle", t.Ref)
		}
		if isIgnorableBaseFacetRef(t.Ref) {
			return nil
		}

		const prefix = "#/$defs/"
		if strings.HasPrefix(t.Ref, prefix) {
			name := strings.TrimPrefix(t.Ref, prefix)
			if def, ok := r.defs[name]; ok {
				t = def
				continue
			}
			return nil
		}

		return nil
	}

	return t
}

// InverseMap returns a map from *schemas.Type pointer → definition name.
// Used by the IR builder to assign TypeName to ObjectDef instances that
// correspond to named $defs entries.
func (r *Resolver) InverseMap() map[*schemas.Type]string {
	m := make(map[*schemas.Type]string, len(r.defs))
	for name, t := range r.defs {
		m[t] = name
	}
	return m
}

func isIgnorableBaseFacetRef(ref string) bool {
	if !strings.Contains(ref, "OpenLineage.json#/$defs/") {
		return false
	}
	switch {
	case strings.HasSuffix(ref, "/DatasetFacet"),
		strings.HasSuffix(ref, "/InputDatasetFacet"),
		strings.HasSuffix(ref, "/OutputDatasetFacet"),
		strings.HasSuffix(ref, "/JobFacet"),
		strings.HasSuffix(ref, "/RunFacet"):
		return true
	}
	return false
}

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

// Package main is the entry point for the OpenLineage client facet code generator.
package main

import (
	"log"
	"os"

	"github.com/OpenLineage/openlineage/generator/go/render/client"
	"github.com/atombender/go-jsonschema/pkg/schemas"

	"github.com/OpenLineage/openlineage/generator/go/discover"
	"github.com/OpenLineage/openlineage/generator/go/ir"
	"github.com/OpenLineage/openlineage/generator/go/resolve"
)

func main() {
	globs := []string{
		"../../spec/facets/*Facet.json",
		"../../spec/registry/*/facets/*Facet.json",
		"../../spec/registry/*/*/facets/*Facet.json",
	}

	paths, err := discover.ResolveGlobs(globs)
	if err != nil {
		log.Fatal(err)
	}

	definitions := map[string]*schemas.Type{}
	var discoveredOL []discover.Facet

	for _, path := range paths {
		loader := schemas.NewDefaultCacheLoader([]string{"json"}, nil)
		schema, err := loader.Load(path, "")
		if err != nil {
			log.Fatalf("load schema %s: %v", path, err)
		}
		for name, def := range schema.Definitions {
			if _, ok := definitions[name]; !ok {
				definitions[name] = def
			}
		}
		discoveredOL = append(discoveredOL, discover.FindAllFacets(schema)...)
	}

	resolver := resolve.New(definitions)

	// OL client facets: all kinds, no field filtering
	var olFacets []ir.Facet
	for _, f := range discoveredOL {
		olFacets = append(olFacets, ir.BuildFacet(f, resolver, false))
	}

	outDir := "../../client/go/pkg/facets"
	_ = os.MkdirAll(outDir, 0o755)

	write(outDir+"/facets.gen.go", client.RenderStructs(olFacets))
	write(outDir+"/facet_helpers.gen.go", client.RenderHelpers(olFacets))
}

func write(path, contents string) {
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		log.Fatal(err)
	}
}

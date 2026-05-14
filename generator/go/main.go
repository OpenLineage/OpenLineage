/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

// Package main is the entry point for the OpenLineage client facet code generator.
package main

import (
	"go/format"
	"log"
	"os"
	"reflect"

	"github.com/atombender/go-jsonschema/pkg/schemas"

	"github.com/OpenLineage/openlineage/generator/go/discover"
	"github.com/OpenLineage/openlineage/generator/go/ir"
	"github.com/OpenLineage/openlineage/generator/go/render/client"
	"github.com/OpenLineage/openlineage/generator/go/render/spec"
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

	loader := schemas.NewDefaultCacheLoader([]string{"json"}, nil)
	for _, path := range paths {
		schema, err := loader.Load(path, "")
		if err != nil {
			log.Fatalf("load schema %s: %v", path, err)
		}
		for name, def := range schema.Definitions {
			if existing, ok := definitions[name]; ok {
				if !reflect.DeepEqual(existing, def) {
					log.Fatalf("conflicting $defs %q across schema files (second occurrence in %s)", name, path)
				}
				continue
			}
			definitions[name] = def
		}
		discoveredOL = append(discoveredOL, discover.FindAllFacets(schema)...)
	}

	resolver := resolve.New(definitions)

	// OL client facets: all kinds, no field filtering
	var olFacets []ir.Facet
	for _, f := range discoveredOL {
		olFacets = append(olFacets, ir.BuildFacet(f, resolver, false))
	}

	facetsDir := "../../client/go/pkg/facets"
	_ = os.MkdirAll(facetsDir, 0o755)
	write(facetsDir+"/facets.gen.go", client.RenderStructs(olFacets))
	write(facetsDir+"/facet_helpers.gen.go", client.RenderHelpers(olFacets))

	// Core spec types from OpenLineage.json
	specLoader := schemas.NewDefaultCacheLoader([]string{"json"}, nil)
	specSchema, err := specLoader.Load("../../spec/OpenLineage.json", "")
	if err != nil {
		log.Fatalf("load OpenLineage.json: %v", err)
	}
	olDir := "../../client/go/pkg/openlineage"
	_ = os.MkdirAll(olDir, 0o755)
	write(olDir+"/spec.gen.go", spec.RenderSpec(specSchema))
}

func write(path, contents string) {
	formatted, err := format.Source([]byte(contents))
	if err != nil {
		log.Fatalf("format %s: %v\n----\n%s", path, err, contents)
	}
	if err := os.WriteFile(path, formatted, 0o644); err != nil {
		log.Fatal(err)
	}
}

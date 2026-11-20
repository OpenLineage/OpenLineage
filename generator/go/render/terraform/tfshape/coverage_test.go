/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package tfshape_test

import (
	"reflect"
	"slices"
	"testing"

	"github.com/atombender/go-jsonschema/pkg/schemas"

	"github.com/OpenLineage/openlineage/generator/go/discover"
	"github.com/OpenLineage/openlineage/generator/go/ir"
	"github.com/OpenLineage/openlineage/generator/go/render/terraform/tfshape"
	"github.com/OpenLineage/openlineage/generator/go/resolve"
)

// specFacets loads the real spec the way main.go does and returns every discovered facet,
// the facets targeted by the Terraform generator, and a resolver.
func specFacets(t *testing.T) (all, terraform []discover.Facet, r *resolve.Resolver) {
	t.Helper()
	paths, err := discover.ResolveGlobs([]string{
		"../../../../../spec/facets/*Facet.json",
		"../../../../../spec/registry/*/facets/*Facet.json",
		"../../../../../spec/registry/*/*/facets/*Facet.json",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no spec facet files found")
	}
	definitions := map[string]*schemas.Type{}
	loader := schemas.NewDefaultCacheLoader([]string{"json"}, nil)
	for _, p := range paths {
		s, err := loader.Load(p, "")
		if err != nil {
			t.Fatalf("load %s: %v", p, err)
		}
		for name, def := range s.Definitions {
			if existing, ok := definitions[name]; ok && !reflect.DeepEqual(existing, def) {
				t.Fatalf("conflicting $defs %q in %s", name, p)
			}
			definitions[name] = def
		}
		found := discover.FindAllFacets(s)
		all = append(all, found...)
		if !discover.IsTerraformExcluded(found) {
			terraform = append(terraform, found...)
		}
	}
	return all, terraform, resolve.New(definitions)
}

// Every Terraform-targeted facet has a supported shape (the generator would stop otherwise,
// and this names the facet and field).
func TestCoverageTargetedFacetsSupported(t *testing.T) {
	_, terraform, r := specFacets(t)
	for _, f := range terraform {
		t.Run(f.Name, func(t *testing.T) {
			defer func() {
				if rec := recover(); rec != nil {
					t.Fatalf("unsupported shape: %v", rec)
				}
			}()
			tfshape.NewIndex(ir.BuildFacet(f, r, true))
		})
	}
}

// Every job and dataset facet in the spec is either generated or excluded by name: a new facet
// can't be dropped silently (for example because its file also holds a run facet).
func TestCoverageEveryFacetGeneratedOrExcluded(t *testing.T) {
	all, terraform, _ := specFacets(t)
	targeted := map[string]bool{}
	for _, f := range terraform {
		targeted[f.Name] = true
	}
	for _, f := range all {
		if f.Type != discover.FacetJob && f.Type != discover.FacetDataset {
			continue
		}
		if !targeted[f.Name] && !slices.Contains(discover.TerraformExcludedFacets, f.Name) {
			t.Errorf("%s (%s facet) is neither generated for Terraform nor in discover.TerraformExcludedFacets", f.Name, f.Type)
		}
	}
}

// Every excluded name still exists in the spec, so the exclusion list doesn't go stale.
func TestCoverageNoStaleExclusions(t *testing.T) {
	all, _, _ := specFacets(t)
	for _, name := range discover.TerraformExcludedFacets {
		if !slices.ContainsFunc(all, func(f discover.Facet) bool { return f.Name == name }) {
			t.Errorf("%s is in discover.TerraformExcludedFacets but not in the spec", name)
		}
	}
}

// Every tfshape.ExcludedFields entry matches a field of a generated facet.
func TestCoverageNoStaleExcludedFields(t *testing.T) {
	_, terraform, r := specFacets(t)
	found := map[string]bool{}
	for _, f := range terraform {
		for _, k := range tfshape.NewIndex(ir.BuildFacet(f, r, true)).ExcludedFound() {
			found[k] = true
		}
	}
	for _, k := range tfshape.ExcludedFields {
		if !found[k] {
			t.Errorf("%s is in tfshape.ExcludedFields but matches no field of a generated facet", k)
		}
	}
}

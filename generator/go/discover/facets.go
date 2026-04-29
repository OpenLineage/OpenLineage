/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

// Package discover provides schema loading and facet discovery from JSON Schema files.
package discover

import (
	"sort"
	"strings"

	"github.com/atombender/go-jsonschema/pkg/schemas"
)

// FacetType identifies the OpenLineage facet category.
type FacetType string

// Facet categories as defined in the OpenLineage specification.
const (
	FacetJob           FacetType = "job"
	FacetDataset       FacetType = "dataset"
	FacetRun           FacetType = "run"
	FacetInputDataset  FacetType = "inputDataset"
	FacetOutputDataset FacetType = "outputDataset"
)

// Facet describes a single OpenLineage facet discovered from a JSON schema file.
type Facet struct {
	Name         string
	Type         FacetType
	Schema       *schemas.Type
	SchemaURL    string // $id of the JSON schema file (e.g. https://…/1-1-0/CatalogDatasetFacet.json)
	ContainerKey string // JSON key used in the facets container struct (e.g. "columnLineage")
}

func detectFacetType(t *schemas.Type) (FacetType, bool) {
	for _, sub := range t.AllOf {
		if sub.Ref == "" {
			continue
		}
		switch {
		case isBaseFacetRef(sub.Ref, "JobFacet"):
			return FacetJob, true
		case isBaseFacetRef(sub.Ref, "DatasetFacet"):
			return FacetDataset, true
		case isBaseFacetRef(sub.Ref, "RunFacet"):
			return FacetRun, true
		case isBaseFacetRef(sub.Ref, "InputDatasetFacet"):
			return FacetInputDataset, true
		case isBaseFacetRef(sub.Ref, "OutputDatasetFacet"):
			return FacetOutputDataset, true
		}
	}
	return "", false
}

// isBaseFacetRef returns true when ref points exactly to the canonical OL base
// facet type (e.g. "…OpenLineage.json#/$defs/JobFacet").
// Using the full path prevents false positives for names like "MyExtensionJobFacet".
func isBaseFacetRef(ref, baseName string) bool {
	return strings.HasSuffix(ref, "OpenLineage.json#/$defs/"+baseName)
}

// containerKeys returns a map from definition name → its top-level container JSON key.
// Each facet schema file has top-level "properties" that register the facet under
// a specific key (e.g. "properties": {"columnLineage": {"$ref": "#/$defs/ColumnLineageDatasetFacet"}}).
func containerKeys(schema *schemas.Schema) map[string]string {
	out := map[string]string{}
	if schema.ObjectAsType == nil {
		return out
	}
	keys := make([]string, 0, len(schema.Properties))
	for k := range schema.Properties {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		prop := schema.Properties[key]
		if prop.Ref != "" {
			name := extractDefName(prop.Ref)
			if name != "" {
				out[name] = key
			}
		}
	}
	return out
}

func extractDefName(ref string) string {
	const suffix = "/$defs/"
	i := strings.LastIndex(ref, suffix)
	if i < 0 {
		return ""
	}
	return ref[i+len(suffix):]
}


// FindAllFacets returns all facet types (job, dataset, run, input, output) without
// any exclusion filter. Used for OL client code generation.
func FindAllFacets(schema *schemas.Schema) []Facet {
	keys := containerKeys(schema)
	var out []Facet

	names := make([]string, 0, len(schema.Definitions))
	for n := range schema.Definitions {
		names = append(names, n)
	}
	sort.Strings(names)

	for _, name := range names {
		def := schema.Definitions[name]
		ft, ok := detectFacetType(def)
		if !ok {
			continue
		}
		out = append(out, Facet{
			Name:         name,
			Type:         ft,
			Schema:       def,
			SchemaURL:    schema.ID,
			ContainerKey: keys[name],
		})
	}
	return out
}


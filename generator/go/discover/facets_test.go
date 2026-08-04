/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package discover

import (
	"reflect"
	"testing"

	"github.com/atombender/go-jsonschema/pkg/schemas"
)

func TestContainerKeysIncludesAllUnionBranches(t *testing.T) {
	schema := &schemas.Schema{
		ID: "https://openlineage.io/spec/facets/1-0-0/LineageFacet.json",
		ObjectAsType: &schemas.ObjectAsType{
			Properties: map[string]*schemas.Type{
				"lineage": {
					AnyOf: []*schemas.Type{
						{Ref: "#/$defs/LineageDatasetFacet"},
						{Ref: "#/$defs/LineageJobFacet"},
					},
				},
			},
		},
		Definitions: schemas.Definitions{
			"LineageDatasetFacet": {
				AllOf: []*schemas.Type{
					{Ref: "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/DatasetFacet"},
				},
			},
			"LineageJobFacet": {
				AllOf: []*schemas.Type{
					{Ref: "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobFacet"},
				},
			},
		},
	}

	want := map[string]string{
		"LineageDatasetFacet": "lineage",
		"LineageJobFacet":     "lineage",
	}
	if got := containerKeys(schema); !reflect.DeepEqual(got, want) {
		t.Fatalf("containerKeys() = %v, want %v", got, want)
	}

	facets := FindAllFacets(schema)
	if len(facets) != 2 {
		t.Fatalf("FindAllFacets() returned %d facets, want 2", len(facets))
	}
	if got, want := facets[0].SchemaURL, schema.ID+"#/$defs/LineageDatasetFacet"; got != want {
		t.Fatalf("dataset facet SchemaURL = %q, want %q", got, want)
	}
	if got, want := facets[1].SchemaURL, schema.ID+"#/$defs/LineageJobFacet"; got != want {
		t.Fatalf("job facet SchemaURL = %q, want %q", got, want)
	}
}

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"testing"

	"github.com/OpenLineage/openlineage/client/go/pkg/openlineage"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

func jobModel(ns, name types.String) *JobResourceModel {
	return &JobResourceModel{OLJobConfig: OLJobConfig{Namespace: ns, Name: name}}
}

// requireErrorAt fails unless diags has exactly one error, at path p.
func requireErrorAt(t *testing.T, diags diag.Diagnostics, p path.Path) {
	t.Helper()
	errs := diags.Errors()
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1: %v", len(errs), diags)
	}
	withPath, ok := errs[0].(diag.DiagnosticWithPath)
	if !ok {
		t.Fatalf("error has no path: %v", errs[0])
	}
	if !withPath.Path().Equal(p) {
		t.Fatalf("error at %s, want %s: %v", withPath.Path(), p, errs[0])
	}
}

func TestBuildJobEventReturnsDiagnostics(t *testing.T) {
	_, diags := BuildJobEvent(jobModel(types.StringValue("ns"), types.StringNull()), EmptyJobCapability(), testProducer)
	requireErrorAt(t, diags, path.Root("name"))
}

func TestBuildDatasetEventReturnsDiagnostics(t *testing.T) {
	m := &DatasetResourceModel{DatasetModel: DatasetModel{Namespace: types.StringNull(), Name: types.StringValue("n")}}
	_, diags := BuildDatasetEvent(m, EmptyDatasetCapability(), testProducer)
	requireErrorAt(t, diags, path.Root("namespace"))
}

func TestBuildRunEventIsCompleteWithFreshRunID(t *testing.T) {
	m := jobModel(types.StringValue("ns"), types.StringValue("job"))
	first, diags := BuildRunEvent(m, EmptyJobCapability(), testProducer)
	requireNoDiags(t, "first", diags)
	second, diags := BuildRunEvent(m, EmptyJobCapability(), testProducer)
	requireNoDiags(t, "second", diags)

	if first.EventType == nil || *first.EventType != openlineage.EventTypeComplete {
		t.Fatalf("event type = %v, want COMPLETE", first.EventType)
	}
	if first.Job.Namespace != "ns" || first.Job.Name != "job" {
		t.Fatalf("job = %s/%s, want ns/job", first.Job.Namespace, first.Job.Name)
	}
	if first.Run.RunID == "" || first.Run.RunID == second.Run.RunID {
		t.Fatalf("run IDs %q and %q: want a new one per call", first.Run.RunID, second.Run.RunID)
	}
}

// A duplicate map key is reported at the second entry's name.
func TestBuildJobEventDuplicateMapEntryPath(t *testing.T) {
	field := func(name, input string) ColumnLineageDatasetFacetModelFields {
		return ColumnLineageDatasetFacetModelFields{
			Name: types.StringValue(name),
			InputFields: []ColumnLineageDatasetFacetModelDataset{{
				Namespace: types.StringValue("s3://raw"),
				Name:      types.StringValue("events"),
				Field:     types.StringValue(input),
			}},
		}
	}
	m := jobModel(types.StringValue("ns"), types.StringValue("job"))
	m.Outputs = []OLOutputModel{{
		DatasetModel: DatasetModel{Namespace: types.StringValue("s3://warehouse"), Name: types.StringValue("out")},
		ColumnLineage: &ColumnLineageDatasetFacetModel{
			Fields: []ColumnLineageDatasetFacetModelFields{field("id", "a"), field("id", "b")},
		},
	}}
	_, diags := BuildJobEvent(m, allOnJobCapability(), testProducer)
	requireErrorAt(t, diags, path.Root("outputs").AtListIndex(0).AtName("column_lineage").AtName("fields").AtListIndex(1).AtName("name"))
}

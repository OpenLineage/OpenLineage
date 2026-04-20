/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
)

// ── stub detection helpers ────────────────────────────────────────────────────

// isStubStringAttr returns true when a StringAttribute is Optional+Computed
// (i.e. a no-op stub for a disabled facet).
func isStubStringAttr(a schema.Attribute) bool {
	sa, ok := a.(schema.StringAttribute)
	return ok && sa.Optional && sa.Computed
}

// isStubBoolAttr returns true when a BoolAttribute is Optional+Computed.
func isStubBoolAttr(a schema.Attribute) bool {
	ba, ok := a.(schema.BoolAttribute)
	return ok && ba.Optional && ba.Computed
}

// allAttrsAreStubs returns true when every attribute in the map is a stub.
func allAttrsAreStubs(attrs map[string]schema.Attribute) bool {
	for _, a := range attrs {
		switch a.(type) {
		case schema.StringAttribute:
			if !isStubStringAttr(a) {
				return false
			}
		case schema.BoolAttribute:
			if !isStubBoolAttr(a) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// isSingleBlockStub returns true when a SingleNestedBlock has all stub attributes
// (and all nested blocks are also stubs).
func isSingleBlockStub(b schema.Block) bool {
	snb, ok := b.(schema.SingleNestedBlock)
	if !ok {
		return false
	}
	if len(snb.Attributes) > 0 && !allAttrsAreStubs(snb.Attributes) {
		return false
	}
	for _, nested := range snb.Blocks {
		if !isBlockStub(nested) {
			return false
		}
	}
	return true
}

// isListBlockStub returns true when a ListNestedBlock has all stub attributes.
func isListBlockStub(b schema.Block) bool {
	lnb, ok := b.(schema.ListNestedBlock)
	if !ok {
		return false
	}
	if len(lnb.NestedObject.Attributes) > 0 && !allAttrsAreStubs(lnb.NestedObject.Attributes) {
		return false
	}
	for _, nested := range lnb.NestedObject.Blocks {
		if !isBlockStub(nested) {
			return false
		}
	}
	return true
}

func isBlockStub(b schema.Block) bool {
	switch b.(type) {
	case schema.SingleNestedBlock:
		return isSingleBlockStub(b)
	case schema.ListNestedBlock:
		return isListBlockStub(b)
	}
	return false
}

// hasActiveStringAttr returns true when at least one attribute in the map is
// Optional+NotComputed — the signature of an enabled (active) facet attribute
// after Option A: formerly-Required attrs become Optional with a validator,
// keeping Computed=false so removed blocks still produce a plan diff.
func hasActiveStringAttr(attrs map[string]schema.Attribute) bool {
	for _, a := range attrs {
		if sa, ok := a.(schema.StringAttribute); ok && sa.Optional && !sa.Computed {
			return true
		}
	}
	return false
}

// ── GenerateJobSchema — identity attributes ───────────────────────────────────

func TestGenerateJobSchema_AlwaysHasNamespaceAttribute(t *testing.T) {
	s := GenerateJobSchema(EmptyJobCapability())

	a, ok := s.Attributes["namespace"]
	if !ok {
		t.Fatal("expected 'namespace' attribute in job schema")
	}
	sa, ok := a.(schema.StringAttribute)
	if !ok {
		t.Fatal("expected 'namespace' to be a StringAttribute")
	}
	if !sa.Required {
		t.Error("expected 'namespace' to be Required")
	}
}

func TestGenerateJobSchema_AlwaysHasNameAttribute(t *testing.T) {
	s := GenerateJobSchema(EmptyJobCapability())

	a, ok := s.Attributes["name"]
	if !ok {
		t.Fatal("expected 'name' attribute in job schema")
	}
	sa, ok := a.(schema.StringAttribute)
	if !ok {
		t.Fatal("expected 'name' to be a StringAttribute")
	}
	if !sa.Required {
		t.Error("expected 'name' to be Required")
	}
}

func TestGenerateJobSchema_AlwaysHasDescriptionAttribute(t *testing.T) {
	s := GenerateJobSchema(EmptyJobCapability())

	a, ok := s.Attributes["description"]
	if !ok {
		t.Fatal("expected 'description' attribute in job schema")
	}
	sa, ok := a.(schema.StringAttribute)
	if !ok {
		t.Fatal("expected 'description' to be a StringAttribute")
	}
	if !sa.Optional {
		t.Error("expected 'description' to be Optional")
	}
}

// ── GenerateJobSchema — inputs / outputs always present ───────────────────────

func TestGenerateJobSchema_AlwaysHasInputsBlock(t *testing.T) {
	s := GenerateJobSchema(EmptyJobCapability())

	if _, ok := s.Blocks["inputs"]; !ok {
		t.Error("expected 'inputs' block in job schema")
	}
}

func TestGenerateJobSchema_AlwaysHasOutputsBlock(t *testing.T) {
	s := GenerateJobSchema(EmptyJobCapability())

	if _, ok := s.Blocks["outputs"]; !ok {
		t.Error("expected 'outputs' block in job schema")
	}
}

// ── GenerateJobSchema — job facet stubs vs. full blocks ──────────────────────

var allJobFacetKeys = []struct {
	facet JobFacet
	key   string
}{
	{FacetJobType, "job_type"},
	{FacetJobOwnership, "ownership"},
	{FacetJobDocumentation, "documentation"},
	{FacetJobSourceCode, "source_code"},
	{FacetJobSourceCodeLocation, "source_code_location"},
	{FacetJobSQL, "sql"},
	{FacetJobTags, "tags"},
}

func TestGenerateJobSchema_AllJobFacetBlocksPresent(t *testing.T) {
	// Every known job facet block must appear in the schema regardless of capability.
	s := GenerateJobSchema(EmptyJobCapability())

	for _, tc := range allJobFacetKeys {
		if _, ok := s.Blocks[tc.key]; !ok {
			t.Errorf("expected block %q to be present (as stub) when facet is disabled", tc.key)
		}
	}
}

func TestGenerateJobSchema_DisabledJobFacetIsStub(t *testing.T) {
	s := GenerateJobSchema(EmptyJobCapability()) // nothing enabled

	for _, tc := range allJobFacetKeys {
		b, ok := s.Blocks[tc.key]
		if !ok {
			t.Errorf("block %q not found", tc.key)
			continue
		}
		if !isBlockStub(b) {
			t.Errorf("block %q should be a stub when %v is disabled", tc.key, tc.facet)
		}
	}
}

func TestGenerateJobSchema_EnabledJobFacetIsNotStub(t *testing.T) {
	for _, tc := range allJobFacetKeys {
		cap := EmptyJobCapability().WithFacetEnabled(tc.facet)
		s := GenerateJobSchema(cap)

		b, ok := s.Blocks[tc.key]
		if !ok {
			t.Errorf("block %q not found", tc.key)
			continue
		}
		if isBlockStub(b) {
			t.Errorf("block %q should NOT be a stub when %v is enabled", tc.key, tc.facet)
		}
	}
}

func TestGenerateJobSchema_EnablingOneFacetDoesNotActivateOthers(t *testing.T) {
	// Enable only job_type — all other facets must remain stubs.
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobType)
	s := GenerateJobSchema(cap)

	for _, tc := range allJobFacetKeys {
		if tc.facet == FacetJobType {
			continue // skip the one we enabled
		}
		b, ok := s.Blocks[tc.key]
		if !ok {
			t.Errorf("block %q not found", tc.key)
			continue
		}
		if !isBlockStub(b) {
			t.Errorf("block %q should still be a stub when only FacetJobType is enabled", tc.key)
		}
	}
}

// ── GenerateJobSchema — dataset facet stubs in inputs/outputs ────────────────

var allDatasetFacetKeys = []struct {
	facet DatasetFacet
	key   string
}{
	{FacetDatasetSymlinks, "symlinks"},
	{FacetDatasetSchema, "schema"},
	{FacetDatasetDataSource, "data_source"},
	{FacetDatasetDocumentation, "documentation"},
	{FacetDatasetType, "dataset_type"},
	{FacetDatasetVersion, "version"},
	{FacetDatasetStorage, "storage"},
	{FacetDatasetOwnership, "ownership"},
	{FacetDatasetLifecycleStateChange, "lifecycle_state_change"},
	{FacetDatasetHierarchy, "hierarchy"},
	{FacetDatasetCatalog, "catalog"},
	{FacetDatasetTags, "tags"},
}

func TestGenerateJobSchema_InputsContainsAllDatasetFacetBlocks(t *testing.T) {
	s := GenerateJobSchema(EmptyJobCapability())
	inputs, ok := s.Blocks["inputs"].(schema.ListNestedBlock)
	if !ok {
		t.Fatal("expected 'inputs' to be a ListNestedBlock")
	}

	for _, tc := range allDatasetFacetKeys {
		if _, ok := inputs.NestedObject.Blocks[tc.key]; !ok {
			t.Errorf("expected dataset facet block %q in inputs", tc.key)
		}
	}
}

func TestGenerateJobSchema_OutputsContainsAllDatasetFacetBlocks(t *testing.T) {
	s := GenerateJobSchema(EmptyJobCapability())
	outputs, ok := s.Blocks["outputs"].(schema.ListNestedBlock)
	if !ok {
		t.Fatal("expected 'outputs' to be a ListNestedBlock")
	}

	for _, tc := range allDatasetFacetKeys {
		if _, ok := outputs.NestedObject.Blocks[tc.key]; !ok {
			t.Errorf("expected dataset facet block %q in outputs", tc.key)
		}
	}
}

func TestGenerateJobSchema_DisabledDatasetFacetIsStubInInputs(t *testing.T) {
	s := GenerateJobSchema(EmptyJobCapability())
	inputs := s.Blocks["inputs"].(schema.ListNestedBlock)

	for _, tc := range allDatasetFacetKeys {
		b, ok := inputs.NestedObject.Blocks[tc.key]
		if !ok {
			t.Errorf("block %q not found in inputs", tc.key)
			continue
		}
		if !isBlockStub(b) {
			t.Errorf("input block %q should be a stub when %v is disabled", tc.key, tc.facet)
		}
	}
}

func TestGenerateJobSchema_EnabledDatasetFacetIsNotStubInInputs(t *testing.T) {
	for _, tc := range allDatasetFacetKeys {
		cap := EmptyJobCapability().WithDatasetFacetEnabled(tc.facet)
		s := GenerateJobSchema(cap)
		inputs := s.Blocks["inputs"].(schema.ListNestedBlock)

		b, ok := inputs.NestedObject.Blocks[tc.key]
		if !ok {
			t.Errorf("block %q not found in inputs", tc.key)
			continue
		}
		if isBlockStub(b) {
			t.Errorf("input block %q should NOT be a stub when %v is enabled", tc.key, tc.facet)
		}
	}
}

// ── column_lineage in outputs ─────────────────────────────────────────────────

func TestGenerateJobSchema_ColumnLineageBlock_AlwaysPresentInOutputs(t *testing.T) {
	s := GenerateJobSchema(EmptyJobCapability())
	outputs := s.Blocks["outputs"].(schema.ListNestedBlock)

	if _, ok := outputs.NestedObject.Blocks["column_lineage"]; !ok {
		t.Error("expected 'column_lineage' block to be present in outputs (as stub)")
	}
}

func TestGenerateJobSchema_ColumnLineageBlock_IsStubWhenDisabled(t *testing.T) {
	s := GenerateJobSchema(EmptyJobCapability())
	outputs := s.Blocks["outputs"].(schema.ListNestedBlock)

	b := outputs.NestedObject.Blocks["column_lineage"]
	if !isBlockStub(b) {
		t.Error("expected column_lineage to be a stub when FacetDatasetColumnLineage is disabled")
	}
}

func TestGenerateJobSchema_ColumnLineageBlock_IsNotStubWhenEnabled(t *testing.T) {
	cap := EmptyJobCapability().WithDatasetFacetEnabled(FacetDatasetColumnLineage)
	s := GenerateJobSchema(cap)
	outputs := s.Blocks["outputs"].(schema.ListNestedBlock)

	b := outputs.NestedObject.Blocks["column_lineage"]
	if isBlockStub(b) {
		t.Error("expected column_lineage to NOT be a stub when FacetDatasetColumnLineage is enabled")
	}
}

func TestGenerateJobSchema_ColumnLineageNotPresentInInputs(t *testing.T) {
	// column_lineage is output-only — must not appear in inputs block.
	s := GenerateJobSchema(EmptyJobCapability().WithDatasetFacetEnabled(FacetDatasetColumnLineage))
	inputs := s.Blocks["inputs"].(schema.ListNestedBlock)

	if _, ok := inputs.NestedObject.Blocks["column_lineage"]; ok {
		t.Error("column_lineage must not appear in inputs block")
	}
}

// ── GenerateDatasetSchema — identity attributes ───────────────────────────────

func TestGenerateDatasetSchema_AlwaysHasNamespaceAndName(t *testing.T) {
	s := GenerateDatasetSchema(EmptyDatasetCapability())

	for _, key := range []string{"namespace", "name"} {
		a, ok := s.Attributes[key]
		if !ok {
			t.Fatalf("expected %q attribute in dataset schema", key)
		}
		sa, ok := a.(schema.StringAttribute)
		if !ok {
			t.Fatalf("expected %q to be a StringAttribute", key)
		}
		if !sa.Required {
			t.Errorf("expected %q to be Required", key)
		}
	}
}

// ── GenerateDatasetSchema — dataset facet stubs vs. full blocks ───────────────

func TestGenerateDatasetSchema_AllDatasetFacetBlocksPresent(t *testing.T) {
	s := GenerateDatasetSchema(EmptyDatasetCapability())

	for _, tc := range allDatasetFacetKeys {
		if _, ok := s.Blocks[tc.key]; !ok {
			t.Errorf("expected block %q in dataset schema (as stub)", tc.key)
		}
	}
}

func TestGenerateDatasetSchema_DisabledFacetIsStub(t *testing.T) {
	s := GenerateDatasetSchema(EmptyDatasetCapability())

	for _, tc := range allDatasetFacetKeys {
		b, ok := s.Blocks[tc.key]
		if !ok {
			t.Errorf("block %q not found", tc.key)
			continue
		}
		if !isBlockStub(b) {
			t.Errorf("block %q should be a stub when %v is disabled", tc.key, tc.facet)
		}
	}
}

func TestGenerateDatasetSchema_EnabledFacetIsNotStub(t *testing.T) {
	for _, tc := range allDatasetFacetKeys {
		cap := EmptyDatasetCapability().WithFacetEnabled(tc.facet)
		s := GenerateDatasetSchema(cap)

		b, ok := s.Blocks[tc.key]
		if !ok {
			t.Errorf("block %q not found", tc.key)
			continue
		}
		if isBlockStub(b) {
			t.Errorf("block %q should NOT be a stub when %v is enabled", tc.key, tc.facet)
		}
	}
}

func TestGenerateDatasetSchema_EnablingOneFacetDoesNotActivateOthers(t *testing.T) {
	cap := EmptyDatasetCapability().WithFacetEnabled(FacetDatasetSchema)
	s := GenerateDatasetSchema(cap)

	for _, tc := range allDatasetFacetKeys {
		if tc.facet == FacetDatasetSchema {
			continue
		}
		b, ok := s.Blocks[tc.key]
		if !ok {
			t.Errorf("block %q not found", tc.key)
			continue
		}
		if !isBlockStub(b) {
			t.Errorf("block %q should still be a stub when only FacetDatasetSchema is enabled", tc.key)
		}
	}
}

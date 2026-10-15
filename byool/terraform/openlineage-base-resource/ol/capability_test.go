/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import "testing"

// ── EmptyJobCapability ────────────────────────────────────────────────────────

func TestEmptyJobCapability_NoJobFacetsEnabled(t *testing.T) {
	cap := EmptyJobCapability()
	all := []JobFacet{
		FacetJobType, FacetJobOwnership, FacetJobDocumentation,
		FacetJobSourceCode, FacetJobSourceCodeLocation, FacetJobSQL, FacetJobTags,
	}
	for _, f := range all {
		if cap.IsEnabled(f) {
			t.Errorf("EmptyJobCapability: expected job facet %d to be disabled", f)
		}
	}
}

func TestEmptyJobCapability_NoDatasetFacetsEnabled(t *testing.T) {
	cap := EmptyJobCapability()
	all := []DatasetFacet{
		FacetDatasetSymlinks, FacetDatasetSchema, FacetDatasetDataSource,
		FacetDatasetDocumentation, FacetDatasetType, FacetDatasetVersion,
		FacetDatasetStorage, FacetDatasetOwnership, FacetDatasetLifecycleStateChange,
		FacetDatasetHierarchy, FacetDatasetCatalog, FacetDatasetColumnLineage, FacetDatasetTags,
	}
	for _, f := range all {
		if cap.IsDatasetEnabled(f) {
			t.Errorf("EmptyJobCapability: expected dataset facet %d to be disabled", f)
		}
	}
}

// ── WithFacetEnabled ──────────────────────────────────────────────────────────

func TestJobCapability_WithFacetEnabled_ActivatesSingleFacet(t *testing.T) {
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobType)

	if !cap.IsEnabled(FacetJobType) {
		t.Error("expected FacetJobType to be enabled")
	}
	if cap.IsEnabled(FacetJobOwnership) {
		t.Error("expected FacetJobOwnership to remain disabled")
	}
}

func TestJobCapability_WithFacetEnabled_ActivatesMultipleFacetsAtOnce(t *testing.T) {
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobType, FacetJobOwnership, FacetJobSQL)

	if !cap.IsEnabled(FacetJobType) {
		t.Error("expected FacetJobType to be enabled")
	}
	if !cap.IsEnabled(FacetJobOwnership) {
		t.Error("expected FacetJobOwnership to be enabled")
	}
	if !cap.IsEnabled(FacetJobSQL) {
		t.Error("expected FacetJobSQL to be enabled")
	}
	// facet not passed must remain disabled
	if cap.IsEnabled(FacetJobDocumentation) {
		t.Error("expected FacetJobDocumentation to remain disabled")
	}
}

func TestJobCapability_WithFacetEnabled_IsImmutable(t *testing.T) {
	// WithFacetEnabled must return a new value and not mutate the receiver.
	original := EmptyJobCapability()
	_ = original.WithFacetEnabled(FacetJobType)

	if original.IsEnabled(FacetJobType) {
		t.Error("WithFacetEnabled must not mutate the original capability")
	}
}

func TestJobCapability_WithFacetEnabled_ChainPreservesAllFacets(t *testing.T) {
	cap := EmptyJobCapability().
		WithFacetEnabled(FacetJobType).
		WithFacetEnabled(FacetJobOwnership)

	if !cap.IsEnabled(FacetJobType) {
		t.Error("expected FacetJobType to survive chaining")
	}
	if !cap.IsEnabled(FacetJobOwnership) {
		t.Error("expected FacetJobOwnership to be enabled after chaining")
	}
}

// ── WithDatasetFacetEnabled ───────────────────────────────────────────────────

func TestJobCapability_WithDatasetFacetEnabled_ActivatesSingleFacet(t *testing.T) {
	cap := EmptyJobCapability().WithDatasetFacetEnabled(FacetDatasetSymlinks)

	if !cap.IsDatasetEnabled(FacetDatasetSymlinks) {
		t.Error("expected FacetDatasetSymlinks to be enabled")
	}
	if cap.IsDatasetEnabled(FacetDatasetSchema) {
		t.Error("expected FacetDatasetSchema to remain disabled")
	}
}

func TestJobCapability_WithDatasetFacetEnabled_DoesNotAffectJobFacets(t *testing.T) {
	cap := EmptyJobCapability().
		WithDatasetFacetEnabled(FacetDatasetSymlinks, FacetDatasetCatalog)

	if cap.IsEnabled(FacetJobType) {
		t.Error("enabling dataset facets must not enable job facets")
	}
}

func TestJobCapability_WithFacetEnabled_DoesNotAffectDatasetFacets(t *testing.T) {
	cap := EmptyJobCapability().
		WithFacetEnabled(FacetJobType, FacetJobOwnership)

	if cap.IsDatasetEnabled(FacetDatasetSymlinks) {
		t.Error("enabling job facets must not enable dataset facets")
	}
}

func TestJobCapability_MixedFacetsWorkIndependently(t *testing.T) {
	cap := EmptyJobCapability().
		WithFacetEnabled(FacetJobType, FacetJobOwnership).
		WithDatasetFacetEnabled(FacetDatasetSymlinks, FacetDatasetCatalog, FacetDatasetColumnLineage)

	// job facets
	if !cap.IsEnabled(FacetJobType) {
		t.Error("FacetJobType expected enabled")
	}
	if !cap.IsEnabled(FacetJobOwnership) {
		t.Error("FacetJobOwnership expected enabled")
	}
	if cap.IsEnabled(FacetJobSQL) {
		t.Error("FacetJobSQL expected disabled")
	}

	// dataset facets
	if !cap.IsDatasetEnabled(FacetDatasetSymlinks) {
		t.Error("FacetDatasetSymlinks expected enabled")
	}
	if !cap.IsDatasetEnabled(FacetDatasetCatalog) {
		t.Error("FacetDatasetCatalog expected enabled")
	}
	if !cap.IsDatasetEnabled(FacetDatasetColumnLineage) {
		t.Error("FacetDatasetColumnLineage expected enabled")
	}
	if cap.IsDatasetEnabled(FacetDatasetSchema) {
		t.Error("FacetDatasetSchema expected disabled")
	}
}

// ── EmptyDatasetCapability ────────────────────────────────────────────────────

func TestEmptyDatasetCapability_NoFacetsEnabled(t *testing.T) {
	cap := EmptyDatasetCapability()
	all := []DatasetFacet{
		FacetDatasetSymlinks, FacetDatasetSchema, FacetDatasetDataSource,
		FacetDatasetDocumentation, FacetDatasetType, FacetDatasetVersion,
		FacetDatasetStorage, FacetDatasetOwnership, FacetDatasetLifecycleStateChange,
		FacetDatasetHierarchy, FacetDatasetCatalog, FacetDatasetColumnLineage, FacetDatasetTags,
	}
	for _, f := range all {
		if cap.IsEnabled(f) {
			t.Errorf("EmptyDatasetCapability: expected facet %d to be disabled", f)
		}
	}
}

func TestDatasetCapability_WithFacetEnabled_ActivatesFacets(t *testing.T) {
	cap := EmptyDatasetCapability().WithFacetEnabled(FacetDatasetSchema, FacetDatasetSymlinks)

	if !cap.IsEnabled(FacetDatasetSchema) {
		t.Error("FacetDatasetSchema expected enabled")
	}
	if !cap.IsEnabled(FacetDatasetSymlinks) {
		t.Error("FacetDatasetSymlinks expected enabled")
	}
	if cap.IsEnabled(FacetDatasetStorage) {
		t.Error("FacetDatasetStorage expected disabled")
	}
}

func TestDatasetCapability_IsDatasetEnabled_IsAliasForIsEnabled(t *testing.T) {
	cap := EmptyDatasetCapability().WithFacetEnabled(FacetDatasetCatalog)

	if cap.IsEnabled(FacetDatasetCatalog) != cap.IsDatasetEnabled(FacetDatasetCatalog) {
		t.Error("IsEnabled and IsDatasetEnabled must return the same result on DatasetCapability")
	}
	if cap.IsEnabled(FacetDatasetSchema) != cap.IsDatasetEnabled(FacetDatasetSchema) {
		t.Error("IsEnabled and IsDatasetEnabled must agree on disabled facets too")
	}
}

func TestDatasetCapability_WithFacetEnabled_IsImmutable(t *testing.T) {
	original := EmptyDatasetCapability()
	_ = original.WithFacetEnabled(FacetDatasetSchema)

	if original.IsEnabled(FacetDatasetSchema) {
		t.Error("WithFacetEnabled must not mutate the original DatasetCapability")
	}
}

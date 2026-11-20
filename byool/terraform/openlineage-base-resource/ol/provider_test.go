/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"fmt"
	"regexp"
	"slices"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

// These tests run real `terraform` (validate, plan, apply, refresh, destroy) against the
// in-memory test provider from testutil_test.go. resource.UnitTest runs without TF_ACC; set
// TF_ACC_TERRAFORM_VERSION (CI) or TF_ACC_TERRAFORM_PATH to choose the binary.

const (
	allFacetsJobKey     = "job/tf-test/all-facets"
	allFacetsDatasetKey = "dataset/s3://warehouse/all-facets"
)

// dig walks decoded JSON by map keys (string) and list indexes (int); nil if absent.
func dig(v any, path ...any) any {
	for _, p := range path {
		switch k := p.(type) {
		case string:
			m, ok := v.(map[string]any)
			if !ok {
				return nil
			}
			v = m[k]
		case int:
			l, ok := v.([]any)
			if !ok || k >= len(l) {
				return nil
			}
			v = l[k]
		}
	}
	return v
}

func facetKeys(v any) []string {
	m, _ := v.(map[string]any)
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// checkEvent asserts on the latest event recorded for key.
func checkEvent(rec *recorder, key string, check func(event map[string]any) error) resource.TestCheckFunc {
	return func(*terraform.State) error {
		ev := rec.last(key)
		if ev == nil {
			return fmt.Errorf("no event recorded for %s", key)
		}
		return check(ev)
	}
}

func expectFacetCount(where string, facets any, want int) error {
	if got := facetKeys(facets); len(got) != want {
		return fmt.Errorf("%s: got %d facets %v, want %d", where, len(got), got, want)
	}
	return nil
}

// (1) Every facet enabled: apply, the post-apply plan is empty (checked by the framework),
// and every facet reaches the event.
func TestProviderAllFacetsEnabled(t *testing.T) {
	rec := newRecorder()
	resource.UnitTest(t, resource.TestCase{
		ProtoV6ProviderFactories: providerFactories(rec, allOnJobCapability(), allOnDatasetCapability()),
		Steps: []resource.TestStep{
			{
				Config: testConfig(t, "all_facets_job.tf"),
				Check: checkEvent(rec, allFacetsJobKey, func(ev map[string]any) error {
					if err := expectFacetCount("job", dig(ev, "job", "facets"), len(JobFacetSchemas)); err != nil {
						return err
					}
					if err := expectFacetCount("input", dig(ev, "inputs", 0, "facets"), len(DatasetFacetSchemas)); err != nil {
						return err
					}
					// outputs also carry column lineage
					return expectFacetCount("output", dig(ev, "outputs", 0, "facets"), len(DatasetFacetSchemas)+1)
				}),
			},
			{
				Config: testConfig(t, "all_facets_dataset.tf"),
				Check: checkEvent(rec, allFacetsDatasetKey, func(ev map[string]any) error {
					return expectFacetCount("dataset", dig(ev, "dataset", "facets"), len(DatasetFacetSchemas))
				}),
			},
		},
	})
}

// (2) Every facet disabled: the same configs are accepted, apply leaves no unknown values in
// state, and no facet reaches the event.
func TestProviderAllFacetsDisabled(t *testing.T) {
	rec := newRecorder()
	resource.UnitTest(t, resource.TestCase{
		ProtoV6ProviderFactories: providerFactories(rec, EmptyJobCapability(), EmptyDatasetCapability()),
		Steps: []resource.TestStep{
			{
				Config: testConfig(t, "all_facets_job.tf"),
				Check: checkEvent(rec, allFacetsJobKey, func(ev map[string]any) error {
					for _, p := range [][]any{{"job", "facets"}, {"inputs", 0, "facets"}, {"outputs", 0, "facets"}} {
						if err := expectFacetCount(fmt.Sprint(p), dig(ev, p...), 0); err != nil {
							return err
						}
					}
					return nil
				}),
			},
			{
				Config: testConfig(t, "all_facets_dataset.tf"),
				Check: checkEvent(rec, allFacetsDatasetKey, func(ev map[string]any) error {
					return expectFacetCount("dataset", dig(ev, "dataset", "facets"), 0)
				}),
			},
		},
	})
}

// (3) A union element with only one variant block: required attributes of the absent variant
// must not be reported.
func TestProviderUnionSingleVariant(t *testing.T) {
	rec := newRecorder()
	resource.UnitTest(t, resource.TestCase{
		ProtoV6ProviderFactories: providerFactories(rec, allOnJobCapability(), allOnDatasetCapability()),
		Steps: []resource.TestStep{{
			Config: testConfig(t, "variant_job_only.tf"),
			Check: checkEvent(rec, "job/tf-test/variant-job-only", func(ev map[string]any) error {
				entry := dig(ev, "job", "facets", "lineage", "entries", 0)
				if got := dig(entry, "type"); got != "JOB" {
					return fmt.Errorf("entry type = %v, want JOB (entry %v)", got, entry)
				}
				if got := dig(entry, "inputs", 0, "name"); got != "upstream" {
					return fmt.Errorf("input name = %v, want upstream", got)
				}
				return nil
			}),
		}},
	})
}

// (4) Optional list blocks can be left out.
func TestProviderOptionalListsOmitted(t *testing.T) {
	rec := newRecorder()
	resource.UnitTest(t, resource.TestCase{
		ProtoV6ProviderFactories: providerFactories(rec, allOnJobCapability(), allOnDatasetCapability()),
		Steps: []resource.TestStep{{
			Config: testConfig(t, "optional_lists_omitted.tf"),
			Check: checkEvent(rec, "job/tf-test/optional-lists-omitted", func(ev map[string]any) error {
				if got := dig(ev, "outputs", 0, "facets", "columnLineage", "fields", "id", "inputFields", 0, "field"); got != "event_id" {
					return fmt.Errorf("column lineage input field = %v, want event_id", got)
				}
				return nil
			}),
		}},
	})
}

// (5) A union element needs exactly one variant block: one passes, none or both fail.
func TestProviderUnionVariantCount(t *testing.T) {
	t.Run("one", func(t *testing.T) {
		resource.UnitTest(t, resource.TestCase{
			ProtoV6ProviderFactories: providerFactories(newRecorder(), allOnJobCapability(), allOnDatasetCapability()),
			Steps: []resource.TestStep{{
				Config:             testConfig(t, "variant_job_only.tf"),
				PlanOnly:           true,
				ExpectNonEmptyPlan: true,
			}},
		})
	})
	for _, name := range []string{"union_both_variants.tf", "union_no_variant.tf"} {
		t.Run(name, func(t *testing.T) {
			resource.UnitTest(t, resource.TestCase{
				ProtoV6ProviderFactories: providerFactories(newRecorder(), allOnJobCapability(), allOnDatasetCapability()),
				Steps: []resource.TestStep{{
					Config:      testConfig(t, name),
					PlanOnly:    true,
					ExpectError: regexp.MustCompile(`(?s)Exactly one of dataset, job must be set`),
				}},
			})
		})
	}
}

// (6) dependentRequired from the spec (lineage job name <-> namespace) is enforced at plan
// time: name without namespace fails, both set passes.
func TestProviderDependentRequired(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		resource.UnitTest(t, resource.TestCase{
			ProtoV6ProviderFactories: providerFactories(newRecorder(), allOnJobCapability(), allOnDatasetCapability()),
			Steps: []resource.TestStep{{
				Config:      testConfig(t, "dependent_required.tf"),
				PlanOnly:    true,
				ExpectError: regexp.MustCompile(`(?s)namespace.*must be (configured|specified)`),
			}},
		})
	})
	t.Run("complete", func(t *testing.T) {
		resource.UnitTest(t, resource.TestCase{
			ProtoV6ProviderFactories: providerFactories(newRecorder(), allOnJobCapability(), allOnDatasetCapability()),
			Steps: []resource.TestStep{{
				Config:             testConfig(t, "variant_job_only.tf"),
				PlanOnly:           true,
				ExpectNonEmptyPlan: true,
			}},
		})
	})
}

// (7) Recursive schema fields reach the event three levels deep.
func TestProviderNestedSchemaFields(t *testing.T) {
	rec := newRecorder()
	resource.UnitTest(t, resource.TestCase{
		ProtoV6ProviderFactories: providerFactories(rec, allOnJobCapability(), allOnDatasetCapability()),
		Steps: []resource.TestStep{{
			Config: testConfig(t, "nested_schema_fields.tf"),
			Check: checkEvent(rec, "dataset/s3://warehouse/nested", func(ev map[string]any) error {
				if got := dig(ev, "dataset", "facets", "schema", "fields", 0, "fields", 0, "fields", 0, "name"); got != "lat" {
					return fmt.Errorf("nested field = %v, want lat (schema facet %v)", got, dig(ev, "dataset", "facets", "schema"))
				}
				return nil
			}),
		}},
	})
}

// (8) Renaming a standalone dataset replaces it, so the consumer deletes the old one.
func TestProviderDatasetRenameReplaces(t *testing.T) {
	rec := newRecorder()
	resource.UnitTest(t, resource.TestCase{
		ProtoV6ProviderFactories: providerFactories(rec, allOnJobCapability(), allOnDatasetCapability()),
		Steps: []resource.TestStep{
			{Config: testConfig(t, "rename_dataset_before.tf")},
			{
				Config: testConfig(t, "rename_dataset_after.tf"),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{
						plancheck.ExpectResourceAction("openlineage_dataset.test", plancheck.ResourceActionReplace),
					},
				},
				Check: func(*terraform.State) error {
					if !slices.Contains(rec.deletedKeys(), datasetKey("s3://warehouse", "before")) {
						return fmt.Errorf("old dataset not deleted; deletes: %v", rec.deletedKeys())
					}
					return nil
				},
			},
		},
	})
}

// (9) Two map entries with the same name are rejected (the diagnostic path is checked in
// event_builder_test.go).
func TestProviderDuplicateMapEntry(t *testing.T) {
	resource.UnitTest(t, resource.TestCase{
		ProtoV6ProviderFactories: providerFactories(newRecorder(), allOnJobCapability(), allOnDatasetCapability()),
		Steps: []resource.TestStep{{
			Config:      testConfig(t, "duplicate_map_entry.tf"),
			ExpectError: regexp.MustCompile(`(?s)Duplicate name.*Name "id" is used more than once`),
		}},
	})
}

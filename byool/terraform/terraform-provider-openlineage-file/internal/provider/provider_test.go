/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package provider

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

// End-to-end: real `terraform` applies examples/main.tf with this provider, writing into a
// temp directory. resource.UnitTest runs without TF_ACC; set TF_ACC_TERRAFORM_VERSION (CI) or
// TF_ACC_TERRAFORM_PATH to choose the binary.

var factories = map[string]func() (tfprotov6.ProviderServer, error){
	"openlineage": providerserver.NewProtocol6WithError(New("test")()),
}

// exampleConfig returns examples/main.tf with a provider block writing to dir. extraProvider
// is added to the provider block; replace maps old → new strings applied to main.tf.
func exampleConfig(t *testing.T, dir, extraProvider string, replace ...string) string {
	t.Helper()
	main, err := os.ReadFile(filepath.Join("..", "..", "examples", "main.tf"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := string(main)
	for i := 0; i+1 < len(replace); i += 2 {
		if !strings.Contains(cfg, replace[i]) {
			t.Fatalf("examples/main.tf has no %q", replace[i])
		}
		cfg = strings.ReplaceAll(cfg, replace[i], replace[i+1])
	}
	return fmt.Sprintf("provider \"openlineage\" {\n  output_dir = %q\n  %s\n}\n\n%s", dir, extraProvider, cfg)
}

func readJSON(path string) (map[string]any, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	return out, json.Unmarshal(raw, &out)
}

func get(v any, keys ...string) any {
	for _, k := range keys {
		m, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = m[k]
	}
	return v
}

func at(v any, i int) any {
	if l, ok := v.([]any); ok && i < len(l) {
		return l[i]
	}
	return nil
}

func first(v any) any { return at(v, 0) }

// expectFacets checks that facets has every key in want and none in absent.
func expectFacets(where string, facets any, want, absent []string) error {
	m, _ := facets.(map[string]any)
	for _, k := range want {
		if _, ok := m[k]; !ok {
			return fmt.Errorf("%s: facet %q missing", where, k)
		}
	}
	for _, k := range absent {
		if _, ok := m[k]; ok {
			return fmt.Errorf("%s: disabled facet %q was written", where, k)
		}
	}
	return nil
}

func TestExampleEndToEnd(t *testing.T) {
	dir := t.TempDir()
	jobFile := filepath.Join(dir, "jobs", "example", "orders_daily.json")
	datasetFile := filepath.Join(dir, "datasets", "s3:%2F%2Fwarehouse", "orders.json")

	checkJob := func(check func(ev map[string]any) error) resource.TestCheckFunc {
		return func(*terraform.State) error {
			ev, err := readJSON(jobFile)
			if err != nil {
				return err
			}
			return check(ev)
		}
	}

	resource.UnitTest(t, resource.TestCase{
		ProtoV6ProviderFactories: factories,
		CheckDestroy: func(*terraform.State) error {
			for _, f := range []string{jobFile, datasetFile} {
				if _, err := os.Stat(f); !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("%s not removed on destroy (stat: %v)", f, err)
				}
			}
			return nil
		},
		Steps: []resource.TestStep{
			{
				// Create: enabled facets are written, disabled ones are not.
				Config: exampleConfig(t, dir, ""),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("openlineage_job.orders_daily", "file_path", jobFile),
					resource.TestCheckResourceAttr("openlineage_dataset.orders", "file_path", datasetFile),
					// disabled facets stay in state as written
					resource.TestCheckResourceAttr("openlineage_job.orders_daily", "source_code.language", "python"),
					checkJob(func(ev map[string]any) error {
						if get(ev, "eventType") != nil {
							return fmt.Errorf("JobEvent has eventType %v", get(ev, "eventType"))
						}
						if err := expectFacets("job", get(ev, "job", "facets"),
							[]string{"documentation", "jobType", "lineage", "ownership", "sql", "sourceCodeLocation", "tags"},
							[]string{"sourceCode"}); err != nil {
							return err
						}
						if err := expectFacets("inputs[0]", get(first(get(ev, "inputs")), "facets"),
							[]string{"schema", "symlinks", "lineage"}, []string{"catalog", "fabric_onelake"}); err != nil {
							return err
						}
						return expectFacets("outputs[0]", get(first(get(ev, "outputs")), "facets"),
							[]string{"columnLineage", "schema"}, []string{"catalog", "fabric_onelake"})
					}),
					func(*terraform.State) error {
						ev, err := readJSON(datasetFile)
						if err != nil {
							return err
						}
						facets := get(ev, "dataset", "facets")
						if err := expectFacets("dataset", facets, []string{"schema", "storage"}, []string{"catalog", "fabric_onelake"}); err != nil {
							return err
						}
						// schema.fields[1] (address) → fields[0] (geo) → fields[0] (lat)
						address := at(get(facets, "schema", "fields"), 1)
						lat := first(get(first(get(address, "fields")), "fields"))
						if get(lat, "name") != "lat" {
							return fmt.Errorf("nested schema field not written: %v", get(facets, "schema"))
						}
						return nil
					},
				),
			},
			{
				// Update: the file is rewritten.
				Config: exampleConfig(t, dir, "", `description  = "Job docs"`, `description  = "Job docs v2"`),
				Check: checkJob(func(ev map[string]any) error {
					if got := get(ev, "job", "facets", "documentation", "description"); got != "Job docs v2" {
						return fmt.Errorf("documentation = %v, want updated", got)
					}
					return nil
				}),
			},
			{
				// job_event_type = "run": the job file holds a COMPLETE RunEvent.
				Config: exampleConfig(t, dir, `job_event_type = "run"`, `description  = "Job docs"`, `description  = "Job docs v3"`),
				Check: checkJob(func(ev map[string]any) error {
					if get(ev, "eventType") != "COMPLETE" || get(ev, "run", "runId") == nil {
						return fmt.Errorf("want a COMPLETE RunEvent, got eventType=%v run=%v", get(ev, "eventType"), get(ev, "run"))
					}
					if got := get(ev, "job", "facets", "documentation", "description"); got != "Job docs v3" {
						return fmt.Errorf("documentation = %v, want updated", got)
					}
					return nil
				}),
			},
		},
	})
}

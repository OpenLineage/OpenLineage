/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-go/tftypes"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// specDir is the repository's spec directory (the module lives in byool/terraform/).
const specDir = "../../../../spec"

// ── Config synthesis ─────────────────────────────────────────────────────────

// synth builds a full config value for a schema: every attribute set, one element per list
// and map, one variant per union element. variant picks which union variant (by index, modulo
// the variant count), so running variant 0..n-1 uses every variant.
type synth struct {
	ctx     context.Context
	variant int
}

func (s synth) schema(sch schema.Schema) tftypes.Value {
	return s.object(sch.Attributes, sch.Blocks, nil)
}

func (s synth) object(attrs map[string]schema.Attribute, blocks map[string]schema.Block, validators []validator.Object) tftypes.Value {
	types := map[string]tftypes.Type{}
	vals := map[string]tftypes.Value{}
	for name, a := range attrs {
		t := a.GetType().TerraformType(s.ctx)
		types[name] = t
		vals[name] = s.attribute(name, t)
	}
	chosen := ""
	if names := unionVariants(validators); len(names) > 0 {
		chosen = names[s.variant%len(names)]
	}
	for name, b := range blocks {
		t := b.Type().TerraformType(s.ctx)
		types[name] = t
		if chosen != "" && name != chosen {
			vals[name] = tftypes.NewValue(t, nil)
			continue
		}
		switch bb := b.(type) {
		case schema.SingleNestedBlock:
			vals[name] = s.object(bb.Attributes, bb.Blocks, bb.Validators)
		case schema.ListNestedBlock:
			elem := s.object(bb.NestedObject.Attributes, bb.NestedObject.Blocks, bb.NestedObject.Validators)
			vals[name] = tftypes.NewValue(t, []tftypes.Value{elem})
		default:
			panic(fmt.Sprintf("synth: block %q of type %T", name, b))
		}
	}
	return tftypes.NewValue(tftypes.Object{AttributeTypes: types}, vals)
}

// attribute returns a sample value that satisfies the spec's formats for that field name.
func (s synth) attribute(name string, t tftypes.Type) tftypes.Value {
	switch {
	case t.Is(tftypes.String):
		switch {
		case name == "run_id":
			return tftypes.NewValue(t, "0190f0a0-0000-7000-8000-000000000000")
		case strings.HasSuffix(name, "uri"), strings.HasSuffix(name, "url"):
			return tftypes.NewValue(t, "https://example.com/sample")
		}
		return tftypes.NewValue(t, "sample")
	case t.Is(tftypes.Bool):
		return tftypes.NewValue(t, true)
	case t.Is(tftypes.Number):
		return tftypes.NewValue(t, big.NewFloat(1))
	case t.Is(tftypes.Map{}):
		elem := t.(tftypes.Map).ElementType
		return tftypes.NewValue(t, map[string]tftypes.Value{"key": s.attribute("value", elem)})
	}
	panic(fmt.Sprintf("synth: attribute %q of type %s", name, t))
}

// unionVariants returns the variant block names if validators mark a union element.
func unionVariants(validators []validator.Object) []string {
	for _, v := range validators {
		if u, ok := v.(exactlyOneBlockValidator); ok {
			return u.names
		}
	}
	return nil
}

// maxVariants is the largest union variant count in the schema (at least 1).
func maxVariants(attrs map[string]schema.Attribute, blocks map[string]schema.Block, validators []validator.Object) int {
	n := max(1, len(unionVariants(validators)))
	for _, b := range blocks {
		switch bb := b.(type) {
		case schema.SingleNestedBlock:
			n = max(n, maxVariants(bb.Attributes, bb.Blocks, bb.Validators))
		case schema.ListNestedBlock:
			n = max(n, maxVariants(bb.NestedObject.Attributes, bb.NestedObject.Blocks, bb.NestedObject.Validators))
		}
	}
	return n
}

// ── Spec validation ──────────────────────────────────────────────────────────

// specSchemas compiles spec files by their $id, all loaded from the local spec directory.
type specSchemas struct {
	compiler *jsonschema.Compiler
	cache    map[string]*jsonschema.Schema
}

func loadSpec(t *testing.T) *specSchemas {
	t.Helper()
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	err := filepath.WalkDir(specDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".json") || strings.Contains(p, string(filepath.Separator)+"tests"+string(filepath.Separator)) {
			return err
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
		m, ok := doc.(map[string]any)
		if !ok {
			return nil
		}
		id, _ := m["$id"].(string)
		if id == "" {
			return nil
		}
		return c.AddResource(id, doc)
	})
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	return &specSchemas{compiler: c, cache: map[string]*jsonschema.Schema{}}
}

func (s *specSchemas) validate(url string, v any) error {
	sch, ok := s.cache[url]
	if !ok {
		var err error
		if sch, err = s.compiler.Compile(url); err != nil {
			return fmt.Errorf("compile %s: %w", url, err)
		}
		s.cache[url] = sch
	}
	return sch.Validate(v)
}

// validateFacets validates each facet against the spec schema named by its _schemaURL. A URL
// without a fragment names a whole facet file, whose root validates a facets container, so
// {key: facet} is validated and the key is checked too; a #/$defs/... URL names the facet itself.
func (s *specSchemas) validateFacets(t *testing.T, where string, facets any) {
	t.Helper()
	m, _ := facets.(map[string]any)
	for key, f := range m {
		url, _ := dig(f, "_schemaURL").(string)
		if url == "" {
			t.Errorf("%s.%s: no _schemaURL", where, key)
			continue
		}
		var doc any = map[string]any{key: f}
		if strings.Contains(url, "#") {
			doc = f
		}
		if err := s.validate(url, doc); err != nil {
			t.Errorf("%s.%s does not conform to %s:\n%v", where, key, url, err)
		}
	}
}

func toJSON(t *testing.T, v any) any {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out, err := jsonschema.UnmarshalJSON(strings.NewReader(string(raw)))
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}

func requireNoDiags(t *testing.T, what string, diags diag.Diagnostics) {
	t.Helper()
	if diags.HasError() {
		t.Fatalf("%s: %v", what, diags)
	}
}

// ── Tests ────────────────────────────────────────────────────────────────────

// Every facet, filled from its schema, decodes into the model, builds, and the emitted JSON
// conforms to the spec: everything Terraform can emit is valid OpenLineage.
func TestConformanceJobEvent(t *testing.T) {
	ctx := context.Background()
	spec := loadSpec(t)
	sch := GenerateJobSchema(allOnJobCapability())
	for v := range maxVariants(sch.Attributes, sch.Blocks, nil) {
		t.Run(fmt.Sprintf("variant-%d", v), func(t *testing.T) {
			plan := tfsdk.Plan{Schema: sch, Raw: synth{ctx, v}.schema(sch)}
			var m JobResourceModel
			requireNoDiags(t, "decode", plan.Get(ctx, &m))

			event, diags := BuildJobEvent(&m, allOnJobCapability(), testProducer)
			requireNoDiags(t, "build", diags)
			ev := toJSON(t, event)

			if err := spec.validate("https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent", ev); err != nil {
				t.Errorf("JobEvent does not conform:\n%v", err)
			}
			if err := expectFacetCount("job", dig(ev, "job", "facets"), len(JobFacetSchemas)); err != nil {
				t.Error(err)
			}
			if err := expectFacetCount("input", dig(ev, "inputs", 0, "facets"), len(DatasetFacetSchemas)); err != nil {
				t.Error(err)
			}
			if err := expectFacetCount("output", dig(ev, "outputs", 0, "facets"), len(DatasetFacetSchemas)+1); err != nil {
				t.Error(err)
			}
			spec.validateFacets(t, "job", dig(ev, "job", "facets"))
			spec.validateFacets(t, "inputs[0]", dig(ev, "inputs", 0, "facets"))
			spec.validateFacets(t, "outputs[0]", dig(ev, "outputs", 0, "facets"))
		})
	}
}

func TestConformanceDatasetEvent(t *testing.T) {
	ctx := context.Background()
	spec := loadSpec(t)
	sch := GenerateDatasetSchema(allOnDatasetCapability())
	for v := range maxVariants(sch.Attributes, sch.Blocks, nil) {
		t.Run(fmt.Sprintf("variant-%d", v), func(t *testing.T) {
			plan := tfsdk.Plan{Schema: sch, Raw: synth{ctx, v}.schema(sch)}
			var m DatasetResourceModel
			requireNoDiags(t, "decode", plan.Get(ctx, &m))

			event, diags := BuildDatasetEvent(&m, allOnDatasetCapability(), testProducer)
			requireNoDiags(t, "build", diags)
			ev := toJSON(t, event)

			if err := spec.validate("https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/DatasetEvent", ev); err != nil {
				t.Errorf("DatasetEvent does not conform:\n%v", err)
			}
			if err := expectFacetCount("dataset", dig(ev, "dataset", "facets"), len(DatasetFacetSchemas)); err != nil {
				t.Error(err)
			}
			spec.validateFacets(t, "dataset", dig(ev, "dataset", "facets"))
		})
	}
}

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-go/tftypes"
)

// namedSchema is a job or dataset schema built for one capability setting.
type namedSchema struct {
	name   string
	schema schema.Schema
}

// schemaVariants returns the job and dataset schemas with all facets on, all off, and each
// facet alone on. New facets are covered without changing the test.
func schemaVariants() []namedSchema {
	out := []namedSchema{
		{"job/all-on", GenerateJobSchema(allOnJobCapability())},
		{"job/all-off", GenerateJobSchema(EmptyJobCapability())},
		{"dataset/all-on", GenerateDatasetSchema(allOnDatasetCapability())},
		{"dataset/all-off", GenerateDatasetSchema(EmptyDatasetCapability())},
	}
	for _, s := range JobFacetSchemas {
		out = append(out, namedSchema{"job/only-" + s.Key, GenerateJobSchema(EmptyJobCapability().WithFacetEnabled(s.JobFacet))})
	}
	for _, f := range allDatasetFacets() {
		out = append(out,
			namedSchema{"job/only-dataset-" + datasetFacetKey(f), GenerateJobSchema(EmptyJobCapability().WithDatasetFacetEnabled(f))},
			namedSchema{"dataset/only-" + datasetFacetKey(f), GenerateDatasetSchema(EmptyDatasetCapability().WithFacetEnabled(f))},
		)
	}
	return out
}

func datasetFacetKey(f DatasetFacet) string {
	if f == ColumnLineageDatasetFacet {
		return "column_lineage"
	}
	for _, s := range DatasetFacetSchemas {
		if s.DatasetFacet == f {
			return s.Key
		}
	}
	return "unknown"
}

func TestSchemaValidateImplementation(t *testing.T) {
	for _, s := range schemaVariants() {
		t.Run(s.name, func(t *testing.T) {
			if diags := s.schema.ValidateImplementation(context.Background()); diags.HasError() {
				t.Fatalf("ValidateImplementation: %v", diags)
			}
		})
	}
}

// visit calls fn for every attribute and block below the schema root, with its path.
// parentValidators are the object validators of the block that holds the child.
type schemaVisitor struct {
	attr  func(p path.Path, a schema.Attribute)
	block func(p path.Path, name string, b schema.Block, parentValidators []validator.Object)
}

func (v schemaVisitor) walk(p path.Path, attrs map[string]schema.Attribute, blocks map[string]schema.Block, validators []validator.Object) {
	for name, a := range attrs {
		if v.attr != nil {
			v.attr(p.AtName(name), a)
		}
	}
	for name, b := range blocks {
		bp := p.AtName(name)
		if v.block != nil {
			v.block(bp, name, b, validators)
		}
		switch bb := b.(type) {
		case schema.SingleNestedBlock:
			v.walk(bp, bb.Attributes, bb.Blocks, bb.Validators)
		case schema.ListNestedBlock:
			ep := bp.AtListIndex(0)
			v.walk(ep, bb.NestedObject.Attributes, bb.NestedObject.Blocks, bb.NestedObject.Validators)
		}
	}
}

// Attributes inside facet blocks are never Required: the framework demands them even when the
// enclosing block is absent. Requiredness is enforced by object validators instead.
// Stub attributes (disabled facets) are never Computed: nothing would ever resolve them.
func TestSchemaNestedAttributesOptional(t *testing.T) {
	for _, s := range schemaVariants() {
		t.Run(s.name, func(t *testing.T) {
			schemaVisitor{attr: func(p path.Path, a schema.Attribute) {
				if len(p.Steps()) < 2 {
					return // top-level identity attributes
				}
				// inputs[i].name / namespace: dataset identity directly in a list element, which
				// only exists when the block is written, so Required is safe there.
				if a.IsRequired() && len(p.Steps()) != 3 {
					t.Errorf("%s: nested attribute is Required", p)
				}
				if a.IsComputed() {
					t.Errorf("%s: nested attribute is Computed", p)
				}
			}}.walk(path.Empty(), s.schema.Attributes, s.schema.Blocks, nil)
		})
	}
}

// A list block gets SizeAtLeast(1) exactly when its parent requires it: an omitted list block
// reaches the provider as an empty list, so the size validator is what makes it required, and
// it must not reject an omitted optional list.
func TestSchemaListSizeOnlyWhenRequired(t *testing.T) {
	ctx := context.Background()
	for _, s := range []namedSchema{
		{"job", GenerateJobSchema(allOnJobCapability())},
		{"dataset", GenerateDatasetSchema(allOnDatasetCapability())},
	} {
		t.Run(s.name, func(t *testing.T) {
			schemaVisitor{block: func(p path.Path, name string, b schema.Block, parentValidators []validator.Object) {
				lb, ok := b.(schema.ListNestedBlock)
				if !ok || len(p.Steps()) < 2 {
					return // top-level inputs/outputs are optional lists without validators
				}
				hasSize := false
				for _, v := range lb.Validators {
					if strings.HasPrefix(v.Description(ctx), "list must contain at least") {
						hasSize = true
					}
				}
				required := requiredNames(parentValidators)[name]
				switch {
				case hasSize && !required:
					t.Errorf("%s: optional list block has a size validator", p)
				case !hasSize && required:
					t.Errorf("%s: required list block has no size validator", p)
				}
			}}.walk(path.Empty(), s.schema.Attributes, s.schema.Blocks, nil)
		})
	}
}

// requiredNames returns the sibling names listed in requireFields (AlsoRequires) validators.
func requiredNames(validators []validator.Object) map[string]bool {
	out := map[string]bool{}
	for _, v := range validators {
		rv := reflect.ValueOf(v)
		if rv.Kind() != reflect.Struct {
			continue
		}
		f := rv.FieldByName("PathExpressions")
		if !f.IsValid() {
			continue
		}
		exprs, ok := f.Interface().(path.Expressions)
		if !ok {
			continue
		}
		for _, e := range exprs {
			s := e.String()
			out[s[strings.LastIndex(s, ".")+1:]] = true
		}
	}
	return out
}

// A full config value decodes into the model and encodes back unchanged, for every facet and
// every union variant: model tfsdk tags and nesting match the schema exactly.
func TestSchemaModelRoundTrip(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name  string
		sch   schema.Schema
		model func() any
	}{
		{"job", GenerateJobSchema(allOnJobCapability()), func() any { return &JobResourceModel{} }},
		{"dataset", GenerateDatasetSchema(allOnDatasetCapability()), func() any { return &DatasetResourceModel{} }},
	}
	for _, c := range cases {
		for v := range maxVariants(c.sch.Attributes, c.sch.Blocks, nil) {
			t.Run(fmt.Sprintf("%s/variant-%d", c.name, v), func(t *testing.T) {
				raw := synth{ctx, v}.schema(c.sch)
				m := c.model()
				requireNoDiags(t, "get", tfsdk.State{Schema: c.sch, Raw: raw}.Get(ctx, m))
				out := tfsdk.State{Schema: c.sch, Raw: tftypes.NewValue(c.sch.Type().TerraformType(ctx), nil)}
				requireNoDiags(t, "set", out.Set(ctx, m))
				if !out.Raw.Equal(raw) {
					diffs, _ := raw.Diff(out.Raw)
					t.Fatalf("round trip changed %d values, first: %v", len(diffs), diffs[0])
				}
			})
		}
	}
}

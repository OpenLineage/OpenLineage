/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework-validators/objectvalidator"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
)

// GenerateJobSchema returns the job resource schema. Facets enabled in c get their full
// block; disabled facets get a stub block (see stubBlock).
func GenerateJobSchema(c JobCapability) schema.Schema {
	blocks := map[string]schema.Block{}

	for _, spec := range JobFacetSchemas {
		full := spec.Build()
		if c.IsEnabled(spec.JobFacet) {
			blocks[spec.Key] = full
		} else {
			blocks[spec.Key] = stubBlock(spec.BlockKind, full)
		}
	}

	blocks["inputs"] = inputsBlock(c.IsDatasetEnabled)
	blocks["outputs"] = outputsBlock(c.IsDatasetEnabled)

	return schema.Schema{
		Description: "OpenLineage job resource.",
		Attributes: map[string]schema.Attribute{
			"namespace": identityString("Job namespace"),
			"name":      identityString("Job name"),
		},
		Blocks: blocks,
	}
}

// GenerateDatasetSchema returns the standalone dataset resource schema. Changing the
// dataset's namespace or name replaces the resource, so the consumer deletes the old dataset.
func GenerateDatasetSchema(c DatasetCapability) schema.Schema {
	attrs, blocks := datasetSchema(c.IsEnabled)
	attrs["namespace"] = identityString("Dataset namespace")
	attrs["name"] = identityString("Dataset name")
	return schema.Schema{
		Description: "OpenLineage dataset resource.",
		Attributes:  attrs,
		Blocks:      blocks,
	}
}

// datasetSchema returns the dataset attributes and facet blocks shared by standalone datasets
// and job inputs/outputs.
func datasetSchema(isEnabled func(DatasetFacet) bool) (map[string]schema.Attribute, map[string]schema.Block) {
	attrs := map[string]schema.Attribute{
		"namespace": requiredString("Dataset namespace"),
		"name":      requiredString("Dataset name"),
	}
	blocks := map[string]schema.Block{}

	for _, spec := range DatasetFacetSchemas {
		full := spec.Build()
		if isEnabled(spec.DatasetFacet) {
			blocks[spec.Key] = full
		} else {
			blocks[spec.Key] = stubBlock(spec.BlockKind, full)
		}
	}

	return attrs, blocks
}

// identityString is a required top-level attribute whose change replaces the resource.
func identityString(desc string) schema.StringAttribute {
	return schema.StringAttribute{
		Required:    true,
		Description: desc,
		PlanModifiers: []planmodifier.String{
			stringplanmodifier.RequiresReplace(),
		},
	}
}

func inputsBlock(isEnabled func(DatasetFacet) bool) schema.ListNestedBlock {
	attrs, blocks := datasetSchema(isEnabled)
	return schema.ListNestedBlock{
		Description: "Input datasets",
		NestedObject: schema.NestedBlockObject{
			Attributes: attrs,
			Blocks:     blocks,
		},
	}
}

func outputsBlock(isEnabled func(DatasetFacet) bool) schema.ListNestedBlock {
	attrs, blocks := datasetSchema(isEnabled)

	full := columnLineageDatasetBlock()
	if isEnabled(ColumnLineageDatasetFacet) {
		blocks["column_lineage"] = full
	} else {
		blocks["column_lineage"] = stubSingleBlock(full)
	}

	return schema.ListNestedBlock{
		Description: "Output datasets",
		NestedObject: schema.NestedBlockObject{
			Attributes: attrs,
			Blocks:     blocks,
		},
	}
}

func optionalString(desc string) schema.StringAttribute {
	return schema.StringAttribute{Optional: true, Description: desc}
}

func requiredString(desc string) schema.StringAttribute {
	return schema.StringAttribute{Required: true, Description: desc}
}

// requireFields requires the named sibling attributes and blocks whenever the object is set.
// Generated facet attributes are Optional and rely on this instead of Required, because the
// framework reports a Required attribute as missing even when its enclosing block is absent.
func requireFields(names ...string) []validator.Object {
	exprs := make([]path.Expression, len(names))
	for i, n := range names {
		exprs[i] = path.MatchRelative().AtName(n)
	}
	return []validator.Object{objectvalidator.AlsoRequires(exprs...)}
}

// requiresSiblings requires the named sibling attributes whenever this attribute is set
// (the spec's dependentRequired).
func requiresSiblings(names ...string) []validator.String {
	exprs := make([]path.Expression, len(names))
	for i, n := range names {
		exprs[i] = path.MatchRelative().AtParent().AtName(n)
	}
	return []validator.String{stringvalidator.AlsoRequires(exprs...)}
}

// exactlyOneOf requires exactly one of the named child blocks of a union element to be set;
// each union variant is its own single nested block.
func exactlyOneOf(names ...string) []validator.Object {
	return []validator.Object{exactlyOneBlockValidator{names: names}}
}

// exactlyOneBlockValidator counts only the named children. objectvalidator.ExactlyOneOf
// can't be used on the element: it also counts the element itself, which is always set.
type exactlyOneBlockValidator struct {
	names []string
}

func (v exactlyOneBlockValidator) Description(context.Context) string {
	return fmt.Sprintf("Exactly one of %s must be set.", strings.Join(v.names, ", "))
}

func (v exactlyOneBlockValidator) MarkdownDescription(ctx context.Context) string {
	return v.Description(ctx)
}

func (v exactlyOneBlockValidator) ValidateObject(ctx context.Context, req validator.ObjectRequest, resp *validator.ObjectResponse) {
	if req.ConfigValue.IsNull() || req.ConfigValue.IsUnknown() {
		return
	}
	attrs := req.ConfigValue.Attributes()
	set := 0
	for _, n := range v.names {
		a, ok := attrs[n]
		if !ok {
			continue
		}
		if a.IsUnknown() {
			return // decided once the value is known
		}
		if !a.IsNull() {
			set++
		}
	}
	if set != 1 {
		resp.Diagnostics.AddAttributeError(req.Path, "Invalid block combination", v.Description(ctx))
	}
}

// Stubs: a disabled facet keeps its block shape so configs written for another consumer
// still validate, but every attribute is Optional with no validators. Values are stored in
// state as written and never emitted. Not Computed: Terraform requires state to equal config
// for configured values, and Computed would only leave unknown values for unset ones.

func stubSingleBlock(b schema.SingleNestedBlock) schema.SingleNestedBlock {
	return schema.SingleNestedBlock{
		Description: b.Description,
		Attributes:  stubAttributes(b.Attributes),
		Blocks:      stubBlocks(b.Blocks),
	}
}

func stubListBlock(b schema.ListNestedBlock) schema.ListNestedBlock {
	return schema.ListNestedBlock{
		Description: b.Description,
		NestedObject: schema.NestedBlockObject{
			Attributes: stubAttributes(b.NestedObject.Attributes),
			Blocks:     stubBlocks(b.NestedObject.Blocks),
		},
	}
}

func stubAttributes(in map[string]schema.Attribute) map[string]schema.Attribute {
	if len(in) == 0 {
		return in
	}
	out := make(map[string]schema.Attribute, len(in))
	for k, a := range in {
		switch orig := a.(type) {
		case schema.StringAttribute:
			out[k] = schema.StringAttribute{Optional: true, Description: orig.Description}
		case schema.BoolAttribute:
			out[k] = schema.BoolAttribute{Optional: true, Description: orig.Description}
		case schema.Int64Attribute:
			out[k] = schema.Int64Attribute{Optional: true, Description: orig.Description}
		case schema.Float64Attribute:
			out[k] = schema.Float64Attribute{Optional: true, Description: orig.Description}
		case schema.MapAttribute:
			out[k] = schema.MapAttribute{Optional: true, ElementType: orig.ElementType, Description: orig.Description}
		default:
			panic(fmt.Sprintf("ol: no stub for attribute %q of type %T", k, a))
		}
	}
	return out
}

func stubBlocks(in map[string]schema.Block) map[string]schema.Block {
	if len(in) == 0 {
		return in
	}
	out := make(map[string]schema.Block, len(in))
	for k, b := range in {
		switch orig := b.(type) {
		case schema.SingleNestedBlock:
			out[k] = stubSingleBlock(orig)
		case schema.ListNestedBlock:
			out[k] = stubListBlock(orig)
		default:
			panic(fmt.Sprintf("ol: no stub for block %q of type %T", k, b))
		}
	}
	return out
}

func stubBlock(kind BlockKind, full schema.Block) schema.Block {
	if kind == SingleBlock {
		return stubSingleBlock(full.(schema.SingleNestedBlock))
	}
	return stubListBlock(full.(schema.ListNestedBlock))
}

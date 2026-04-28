/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"github.com/hashicorp/terraform-plugin-framework-validators/objectvalidator"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
)

func GenerateJobSchema(cap JobCapability) schema.Schema {
	blocks := map[string]schema.Block{}

	for _, spec := range JobFacetSchemas {
		full := spec.Build()
		if cap.IsEnabled(spec.JobFacet) {
			blocks[spec.Key] = full
		} else {
			blocks[spec.Key] = stubBlock(spec.BlockKind, full)
		}
	}

	blocks["inputs"] = inputsBlock(cap.IsDatasetEnabled)
	blocks["outputs"] = outputsBlock(cap.IsDatasetEnabled)

	return schema.Schema{
		Description: "OpenLineage job resource.",
		Attributes:  jobIdentityAttributes(),
		Blocks:      blocks,
	}
}

func GenerateDatasetSchema(cap DatasetCapability) schema.Schema {
	attrs, blocks := datasetSchema(cap.IsEnabled)
	return schema.Schema{
		Description: "OpenLineage dataset resource.",
		Attributes:  attrs,
		Blocks:      blocks,
	}
}

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

func jobIdentityAttributes() map[string]schema.Attribute {
	return map[string]schema.Attribute{
		"namespace": schema.StringAttribute{
			Required:    true,
			Description: "Job namespace",
			PlanModifiers: []planmodifier.String{
				stringplanmodifier.RequiresReplace(),
			},
		},
		"name": schema.StringAttribute{
			Required:    true,
			Description: "Job name",
			PlanModifiers: []planmodifier.String{
				stringplanmodifier.RequiresReplace(),
			},
		},
		"description": optionalString("Job description"),
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

	full := schema.SingleNestedBlock{}
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

func requireFields(names ...string) []validator.Object {
	exprs := make([]path.Expression, len(names))
	for i, n := range names {
		exprs[i] = path.MatchRelative().AtName(n)
	}
	return []validator.Object{objectvalidator.AlsoRequires(exprs...)}
}

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
			out[k] = schema.StringAttribute{Optional: true, Computed: true, Description: orig.Description}
		case schema.BoolAttribute:
			out[k] = schema.BoolAttribute{Optional: true, Computed: true, Description: orig.Description}
		case schema.Int64Attribute:
			out[k] = schema.Int64Attribute{Optional: true, Computed: true, Description: orig.Description}
		case schema.MapAttribute:
			out[k] = schema.MapAttribute{
				Optional:    true,
				Computed:    true,
				ElementType: orig.ElementType,
				Description: orig.Description,
			}
		default:
			out[k] = a
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
			out[k] = b
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

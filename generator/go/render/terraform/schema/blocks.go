/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package schema

import (
	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/OpenLineage/openlineage/generator/go/ir"
)

type renderCtx struct {
	seen map[*ir.ObjectDef]bool
}

func Block(obj *ir.ObjectDef) schema.SingleNestedBlock {
	ctx := &renderCtx{
		seen: map[*ir.ObjectDef]bool{},
	}
	return ctx.renderObject(obj)
}

func (ctx *renderCtx) renderObject(
	obj *ir.ObjectDef,
) schema.SingleNestedBlock {

	if ctx.seen[obj] {
		// break recursive cycles
		return schema.SingleNestedBlock{}
	}
	ctx.seen[obj] = true

	return schema.SingleNestedBlock{
		Description: obj.Description,
		Attributes:  ctx.renderAttributes(obj),
		Blocks:      ctx.renderBlocks(obj),
		Validators:  renderValidators(obj),
	}
}

// -----------------------------------------------------------------------------
// Attributes (leaf & primitive types)
// -----------------------------------------------------------------------------
func (ctx *renderCtx) renderAttributes(
	obj *ir.ObjectDef,
) map[string]schema.Attribute {

	out := map[string]schema.Attribute{}

	for _, f := range obj.Fields {
		switch f.Type.(type) {
		case ir.Object:
			continue
		case ir.List:
			if _, ok := f.Type.(ir.List).Elem.(ir.Object); ok {
				continue
			}
		}

		out[f.Name] = renderAttribute(f)
	}

	if len(out) == 0 {
		return nil
	}
	return out
}
func renderAttribute(f ir.Field) schema.Attribute {
	switch t := f.Type.(type) {

	case ir.String:
		return schema.StringAttribute{
			Optional:    !f.Required,
			Required:    f.Required,
			Description: f.Description,
		}

	case ir.Bool:
		return schema.BoolAttribute{
			Optional:    !f.Required,
			Required:    f.Required,
			Description: f.Description,
		}

	case ir.Int:
		return schema.Int64Attribute{
			Optional:    !f.Required,
			Required:    f.Required,
			Description: f.Description,
		}

	case ir.Float:
		return schema.Float64Attribute{
			Optional:    !f.Required,
			Required:    f.Required,
			Description: f.Description,
		}

	case ir.Map:
		return schema.MapAttribute{
			Optional:    true,
			Description: f.Description,
			ElementType: elementType(t.Elem),
		}
	}

	panic("unsupported attribute IR type")
}

// -----------------------------------------------------------------------------
// Blocks (object & list-of-object types)
// -----------------------------------------------------------------------------
func (ctx *renderCtx) renderBlocks(
	obj *ir.ObjectDef,
) map[string]schema.Block {

	blocks := map[string]schema.Block{}

	for _, f := range obj.Fields {
		switch t := f.Type.(type) {

		case ir.Object:
			// ✅ delegate to guarded renderObject
			blocks[f.Name] = ctx.renderObject(t.Object)

		case ir.List:
			if o, ok := t.Elem.(ir.Object); ok {
				blocks[f.Name] = schema.ListNestedBlock{
					NestedObject: schema.NestedBlockObject{
						// ✅ attributes are fine (leaf-only)
						Attributes: ctx.renderAttributes(o.Object),
						// ✅ recursion must go through renderObject
						Blocks: ctx.renderObject(o.Object).Blocks,
					},
				}
			}
		}
	}

	if len(blocks) == 0 {
		return nil
	}
	return blocks
}

// -----------------------------------------------------------------------------
// Validators
// -----------------------------------------------------------------------------

func renderValidators(obj *ir.ObjectDef) []validator.Object {
	if len(obj.Required) == 0 {
		return nil
	}

	names := make([]string, 0, len(obj.Required))
	for n := range obj.Required {
		names = append(names, n)
	}

	return requireFields(names...)
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func isBlockType(t ir.Type) bool {
	switch t.(type) {
	case ir.Object:
		return true
	case ir.List:
		_, ok := t.(ir.List).Elem.(ir.Object)
		return ok
	default:
		return false
	}
}

func elementType(t ir.Type) attr.Type {
	switch t.(type) {
	case ir.String:
		return types.StringType
	case ir.Bool:
		return types.BoolType
	case ir.Int:
		return types.Int64Type
	case ir.Float:
		return types.Float64Type
	default:
		return types.StringType
	}
}

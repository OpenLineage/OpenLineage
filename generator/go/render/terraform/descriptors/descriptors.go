/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package descriptors

import (
	_ "embed"
	"sort"
	"strings"
	"text/template"

	"github.com/OpenLineage/openlineage/generator/go/genutil"
	"github.com/OpenLineage/openlineage/generator/go/ir"
	"github.com/OpenLineage/openlineage/generator/go/render/terraform/tfshape"
)

func RenderFacetSchemaFile(facets []ir.Facet) string {
	jobDescriptors := make([]facetDescriptorEntry, 0)
	datasetDescriptors := make([]facetDescriptorEntry, 0)
	for _, f := range facets {
		entry := facetDescriptorEntry{
			FacetName: f.Name,
			Key:       tfshape.FacetKey(f.Name),
			BuildFunc: genutil.LowerFirst(strings.TrimSuffix(f.Name, "Facet")),
		}
		if strings.Contains(f.Name, "JobFacet") {
			jobDescriptors = append(jobDescriptors, entry)
		}
		// Column lineage is only on outputs; ol/generator.go adds it there (as the model does).
		if strings.Contains(f.Name, "DatasetFacet") && f.Name != "ColumnLineageDatasetFacet" {
			datasetDescriptors = append(datasetDescriptors, entry)
		}
	}

	var blockFuncs []blockFuncEntry
	for _, f := range facets {
		blockFuncs = append(blockFuncs, buildBlockFuncEntry(genutil.LowerFirst(strings.TrimSuffix(f.Name, "Facet")), f))
	}

	var b strings.Builder

	templateData := struct {
		JobDescriptors     []facetDescriptorEntry
		DatasetDescriptors []facetDescriptorEntry
		BlockFuncs         []blockFuncEntry
	}{
		JobDescriptors:     jobDescriptors,
		DatasetDescriptors: datasetDescriptors,
		BlockFuncs:         blockFuncs,
	}

	if err := descriptorsTemplate.Execute(&b, templateData); err != nil {
		panic(err)
	}

	return b.String()
}

type facetDescriptorEntry struct {
	FacetName string
	Key       string
	BuildFunc string
}

type blockFuncEntry struct {
	FuncName string
	Body     string
}

// -----------------------------------------------------------------------------
// Block emitters
// -----------------------------------------------------------------------------

type emitCtx struct {
	idx   *tfshape.Index
	depth tfshape.Depth
}

func buildBlockFuncEntry(fn string, f ir.Facet) blockFuncEntry {
	ctx := &emitCtx{idx: tfshape.NewIndex(f), depth: tfshape.Depth{}}
	var body strings.Builder
	ctx.emitSNB(&body, f.Root, f.Name)
	return blockFuncEntry{FuncName: fn, Body: body.String()}
}

type attrEntry struct {
	Name    string
	Literal string
}

type blockEntry struct {
	Name    string
	Literal string
}

type nestedBlockData struct {
	Kind           string // "single" or "list"
	Required       bool   // list blocks: the field is required, so the list must not be empty
	Description    string
	Attrs          []attrEntry
	Blocks         []blockEntry
	RequiredFields []string
	ExactlyOneOf   []string // union element: names of the variant blocks
}

func (ctx *emitCtx) buildAttrEntry(f ir.Field, path string, requires []string) attrEntry {
	var b strings.Builder
	ctx.emitAttr(&b, f, path, requires)
	return attrEntry{Name: f.Name, Literal: b.String()}
}

func (ctx *emitCtx) buildBlockEntry(f ir.Field, path string) blockEntry {
	var b strings.Builder
	ctx.emitBlockField(&b, f, path)
	return blockEntry{Name: f.Name, Literal: b.String()}
}

// objectBody collects the attributes and blocks of obj (which must already be entered in
// ctx.depth). extraAttrs come first (map key). Fields cut off by the recursion depth are left out.
func (ctx *emitCtx) objectBody(obj *ir.ObjectDef, path string, extraAttrs ...attrEntry) nestedBlockData {
	attrEntries := extraAttrs
	var blockEntries []blockEntry
	dependent := ctx.idx.DependentRequired(obj)
	for _, f := range ctx.idx.Fields(obj) {
		if ctx.depth.IsCut(f.Type) {
			continue
		}
		fp := path + "." + f.Name
		if isAttribute(f.Type) {
			attrEntries = append(attrEntries, ctx.buildAttrEntry(f, fp, dependent[f.Name]))
		} else {
			blockEntries = append(blockEntries, ctx.buildBlockEntry(f, fp))
		}
	}
	return nestedBlockData{
		Description:    obj.Description,
		Attrs:          attrEntries,
		Blocks:         blockEntries,
		RequiredFields: ctx.idx.RequiredKeys(obj),
	}
}

func (ctx *emitCtx) execute(b *strings.Builder, data nestedBlockData) {
	if err := nestedBlockTemplate.Execute(b, data); err != nil {
		panic(err)
	}
}

// enter records obj in ctx.depth; callers have already left out fields that would be cut.
func (ctx *emitCtx) enter(obj *ir.ObjectDef, path string) {
	if !ctx.depth.Enter(obj) {
		tfshape.Fail(path, "internal error: object %s entered beyond the recursion depth", obj.TypeName)
	}
}

func (ctx *emitCtx) emitSNB(b *strings.Builder, obj *ir.ObjectDef, path string) {
	ctx.enter(obj, path)
	defer ctx.depth.Leave(obj)

	data := ctx.objectBody(obj, path)
	data.Kind = "single"
	ctx.execute(b, data)
}

func (ctx *emitCtx) emitLNB(b *strings.Builder, obj *ir.ObjectDef, path string, required bool) {
	ctx.enter(obj, path)
	defer ctx.depth.Leave(obj)

	data := ctx.objectBody(obj, path)
	data.Kind = "list"
	data.Required = required
	ctx.execute(b, data)
}

// emitMapObjLNB renders a Map{Object} as a list of entries; the map key is the "name" attribute.
func (ctx *emitCtx) emitMapObjLNB(b *strings.Builder, obj *ir.ObjectDef, path string, required bool) {
	ctx.enter(obj, path)
	defer ctx.depth.Leave(obj)

	data := ctx.objectBody(obj, path, attrEntry{Name: tfshape.MapKeyField, Literal: `optionalString("Map key")`})
	data.Kind = "list"
	data.Required = required
	data.Description = ""
	data.RequiredFields = append([]string{tfshape.MapKeyField}, data.RequiredFields...)
	sort.Strings(data.RequiredFields)
	ctx.execute(b, data)
}

// emitUnionLNB renders a List{Union}: each element holds one single nested block per variant,
// and exactly one of them must be set. The discriminator is implied by the block.
func (ctx *emitCtx) emitUnionLNB(b *strings.Builder, u *ir.UnionDef, path string, required bool) {
	data := nestedBlockData{Kind: "list", Required: required, Description: u.Description}
	for _, v := range u.Variants {
		name := tfshape.VariantName(v)
		var vb strings.Builder
		ctx.emitSNB(&vb, v.Object, path+"."+name)
		data.Blocks = append(data.Blocks, blockEntry{Name: name, Literal: vb.String()})
		data.ExactlyOneOf = append(data.ExactlyOneOf, name)
	}
	sort.Strings(data.ExactlyOneOf)
	ctx.execute(b, data)
}

// emitAttr emits a schema.Attribute literal for a scalar or Map{scalar} field. Attributes are
// always optional: the framework would demand a Required attribute even when its enclosing
// block is absent, so requiredness is enforced by the object's requireFields validator.
// requires lists the sibling attributes that must be set with this one (dependentRequired).
func (ctx *emitCtx) emitAttr(b *strings.Builder, f ir.Field, path string, requires []string) {
	templateData := struct {
		Kind        string
		Description string
		ElemType    string
		Requires    []string
	}{
		Description: f.Description,
		Requires:    requires,
	}

	switch t := f.Type.(type) {
	case ir.Map:
		templateData.Kind = "Map"
		templateData.ElemType = "types." + t.Elem.TFKind() + "Type"
	case ir.String, ir.Bool, ir.Int, ir.Float:
		templateData.Kind = t.TFKind()
	default:
		tfshape.Fail(path, "attribute type %s", tfshape.Describe(f.Type))
	}

	if err := attrLiteralTemplate.Execute(b, templateData); err != nil {
		panic(err)
	}
}

func (ctx *emitCtx) emitBlockField(b *strings.Builder, f ir.Field, path string) {
	switch t := f.Type.(type) {
	case ir.Object:
		ctx.emitSNB(b, t.Object, path)
		return
	case ir.List:
		switch elem := t.Elem.(type) {
		case ir.Object:
			ctx.emitLNB(b, elem.Object, path, f.Required)
			return
		case ir.Union:
			ctx.emitUnionLNB(b, elem.Union, path, f.Required)
			return
		}
	case ir.Map:
		if o, ok := t.Elem.(ir.Object); ok {
			ctx.emitMapObjLNB(b, o.Object, path, f.Required)
			return
		}
	}
	tfshape.Fail(path, "block type %s", tfshape.Describe(f.Type))
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

// isAttribute reports whether a field becomes a schema.Attribute (scalars and Map{scalar});
// everything else becomes a block. tfshape.NewIndex has already rejected unsupported shapes.
func isAttribute(t ir.Type) bool {
	if m, ok := t.(ir.Map); ok {
		return tfshape.IsScalar(m.Elem)
	}
	return tfshape.IsScalar(t)
}

//go:embed descriptors_file.tmpl
var descriptorsTemplateSource string

var descriptorsTemplate = template.Must(template.New("descriptors_file").Parse(descriptorsTemplateSource))

//go:embed attr_literal.tmpl
var attrLiteralTemplateSource string

var attrLiteralTemplate = template.Must(template.New("attr_literal").Parse(attrLiteralTemplateSource))

//go:embed nested_block.tmpl
var nestedBlockTemplateSource string

var nestedBlockTemplate = template.Must(template.New("nested_block").Parse(nestedBlockTemplateSource))

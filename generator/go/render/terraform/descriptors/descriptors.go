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
)

// importFlags tracks which optional packages are needed in the generated file.

func RenderFacetSchemaFile(facets []ir.Facet) string {
	jobDescriptors := make([]facetDescriptorEntry, 0)
	datasetDescriptors := make([]facetDescriptorEntry, 0)
	for _, f := range facets {
		entry := facetDescriptorEntry{
			FacetName: f.Name,
			Key:       genutil.ToSnake(f.Name),
			BuildFunc: genutil.LowerFirst(strings.TrimSuffix(f.Name, "Facet")),
		}
		if strings.Contains(f.Name, "JobFacet") {
			jobDescriptors = append(jobDescriptors, entry)
		}
		if strings.Contains(f.Name, "DatasetFacet") {
			datasetDescriptors = append(datasetDescriptors, entry)
		}
	}

	var blockFuncs []blockFuncEntry
	for _, f := range facets {
		blockFuncs = append(blockFuncs, buildBlockFuncEntry(genutil.LowerFirst(strings.TrimSuffix(f.Name, "Facet")), f.Root))
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
	seen map[*ir.ObjectDef]bool
}

func buildBlockFuncEntry(fn string, obj *ir.ObjectDef) blockFuncEntry {
	ctx := &emitCtx{seen: make(map[*ir.ObjectDef]bool)}
	var body strings.Builder
	ctx.emitSNB(&body, obj)
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
	Description    string
	Attrs          []attrEntry
	Blocks         []blockEntry
	RequiredFields []string
}

func (ctx *emitCtx) buildAttrEntry(f ir.Field) attrEntry {
	var b strings.Builder
	ctx.emitAttr(&b, f)
	return attrEntry{Name: f.Name, Literal: b.String()}
}

func (ctx *emitCtx) buildBlockEntry(f ir.Field) blockEntry {
	var b strings.Builder
	ctx.emitBlockField(&b, f)
	return blockEntry{Name: f.Name, Literal: b.String()}
}

func (ctx *emitCtx) emitSNB(b *strings.Builder, obj *ir.ObjectDef) {
	if obj == nil || ctx.seen[obj] {
		b.WriteString("schema.SingleNestedBlock{}")
		return
	}
	ctx.seen[obj] = true
	defer func() { delete(ctx.seen, obj) }()

	attrs := primitiveFields(obj)
	blocks := complexFields(obj)

	attrEntries := make([]attrEntry, len(attrs))
	for i, f := range attrs {
		attrEntries[i] = ctx.buildAttrEntry(f)
	}
	blockEntries := make([]blockEntry, len(blocks))
	for i, f := range blocks {
		blockEntries[i] = ctx.buildBlockEntry(f)
	}

	if err := nestedBlockTemplate.Execute(b, nestedBlockData{
		Kind:           "single",
		Description:    obj.Description,
		Attrs:          attrEntries,
		Blocks:         blockEntries,
		RequiredFields: requiredSnakeKeys(obj),
	}); err != nil {
		panic(err)
	}
}

// emitAttr emits a schema.Attribute literal for a primitive field.
func (ctx *emitCtx) emitAttr(b *strings.Builder, f ir.Field) {
	templateData := struct {
		Kind        string
		Required    bool
		Description string
		ElemType    string
	}{
		Required:    f.Required,
		Description: f.Description,
	}

	if t, ok := f.Type.(ir.Map); ok {
		templateData.Kind = "Map"
		templateData.Required = false
		templateData.ElemType = elemTypeStr(t.Elem)
	} else {
		templateData.Kind = f.Type.TFKind()
		if templateData.Kind == "" {
			templateData.Kind = "String"
			templateData.Required = false
		}
	}

	if err := attrLiteralTemplate.Execute(b, templateData); err != nil {
		panic(err)
	}
}

func (ctx *emitCtx) emitBlockField(b *strings.Builder, f ir.Field) {
	switch t := f.Type.(type) {
	case ir.Object:
		ctx.emitSNB(b, t.Object)
	case ir.List:
		if o, ok := t.Elem.(ir.Object); ok {
			ctx.emitLNB(b, o.Object)
		}
	case ir.Map:
		if o, ok := t.Elem.(ir.Object); ok {
			ctx.emitMapObjLNB(b, o.Object)
		}
	}
}

func (ctx *emitCtx) emitLNB(b *strings.Builder, obj *ir.ObjectDef) {
	if ctx.seen[obj] {
		b.WriteString("schema.ListNestedBlock{}")
		return
	}
	ctx.seen[obj] = true
	defer func() { delete(ctx.seen, obj) }()

	attrs := primitiveFields(obj)
	blocks := complexFields(obj)

	attrEntries := make([]attrEntry, len(attrs))
	for i, f := range attrs {
		attrEntries[i] = ctx.buildAttrEntry(f)
	}
	blockEntries := make([]blockEntry, len(blocks))
	for i, f := range blocks {
		blockEntries[i] = ctx.buildBlockEntry(f)
	}

	if err := nestedBlockTemplate.Execute(b, nestedBlockData{
		Kind:           "list",
		Description:    obj.Description,
		Attrs:          attrEntries,
		Blocks:         blockEntries,
		RequiredFields: requiredSnakeKeys(obj),
	}); err != nil {
		panic(err)
	}
}

func (ctx *emitCtx) emitMapObjLNB(b *strings.Builder, obj *ir.ObjectDef) {
	if ctx.seen[obj] {
		b.WriteString("schema.ListNestedBlock{}")
		return
	}
	ctx.seen[obj] = true
	defer func() { delete(ctx.seen, obj) }()

	attrs := primitiveFields(obj)
	blocks := complexFields(obj)

	attrEntries := []attrEntry{{Name: "name", Literal: `requiredString("Map key")`}}
	for _, f := range attrs {
		attrEntries = append(attrEntries, ctx.buildAttrEntry(f))
	}
	blockEntries := make([]blockEntry, len(blocks))
	for i, f := range blocks {
		blockEntries[i] = ctx.buildBlockEntry(f)
	}

	if err := nestedBlockTemplate.Execute(b, nestedBlockData{
		Kind:           "list",
		Attrs:          attrEntries,
		Blocks:         blockEntries,
		RequiredFields: requiredSnakeKeys(obj),
	}); err != nil {
		panic(err)
	}
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

// primitiveFields returns fields that become schema.Attribute entries
// (scalars and Map{primitive}; Object/List{Object}/Map{Object} are skipped).
func primitiveFields(obj *ir.ObjectDef) []ir.Field {
	var out []ir.Field
	for _, f := range obj.Fields {
		switch t := f.Type.(type) {
		case ir.Object:
			continue
		case ir.List:
			continue // lists (even of primitives) are not supported as TF attributes
		case ir.Map:
			if _, ok := t.Elem.(ir.Object); ok {
				continue // Map{Object} → emitted as ListNestedBlock
			}
			out = append(out, f)
		default:
			out = append(out, f)
		}
	}
	return out
}

// complexFields returns fields that become schema.Block entries
// (Object, List{Object}, Map{Object}).
func complexFields(obj *ir.ObjectDef) []ir.Field {
	var out []ir.Field
	for _, f := range obj.Fields {
		switch t := f.Type.(type) {
		case ir.Object:
			out = append(out, f)
		case ir.List:
			if _, ok := t.Elem.(ir.Object); ok {
				out = append(out, f)
			}
		case ir.Map:
			if _, ok := t.Elem.(ir.Object); ok {
				out = append(out, f)
			}
		}
	}
	return out
}

// requiredSnakeKeys converts camelCase required-property names to snake_case and sorts them.
func requiredSnakeKeys(obj *ir.ObjectDef) []string {
	if len(obj.Required) == 0 {
		return nil
	}
	keys := make([]string, 0, len(obj.Required))
	for k := range obj.Required {
		keys = append(keys, genutil.ToSnake(k))
	}
	sort.Strings(keys)
	return keys
}

// elemTypeStr returns the types.XxxType expression for a map element primitive type.
func elemTypeStr(t ir.Type) string {
	if kind := t.TFKind(); kind != "" {
		return "types." + kind + "Type"
	}
	return "types.StringType"
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

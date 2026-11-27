/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package model

import (
	_ "embed"
	"sort"
	"strings"
	"text/template"

	"github.com/OpenLineage/openlineage/generator/go/ir"
	"github.com/OpenLineage/openlineage/generator/go/render/terraform/tfshape"
)

// objectKey identifies a rendered struct: an object at a recursion level (1 for objects that
// aren't recursive; SchemaDatasetFacet.fields gets one struct per level).
type objectKey struct {
	obj   *ir.ObjectDef
	level int
}

type renderCtx struct {
	idx    *tfshape.Index
	depth  tfshape.Depth
	seen   map[objectKey]string
	unions map[*ir.UnionDef]string
	out    *strings.Builder
}

func RenderModelsFile(facets []ir.Facet) string {
	var body strings.Builder
	RenderAllFacets(&body, facets)

	var out strings.Builder
	templateData := struct {
		Body string
	}{
		Body: body.String(),
	}
	if err := modelsFileTemplate.Execute(&out, templateData); err != nil {
		panic(err)
	}
	return out.String()
}

// RenderFacetModels renders all Go model structs for a facet.
// The root struct is named <FacetName>Model, e.g. ColumnLineageDatasetFacetModel.
func RenderFacetModels(b *strings.Builder, f ir.Facet) {
	ctx := &renderCtx{
		idx:    tfshape.NewIndex(f),
		depth:  tfshape.Depth{},
		seen:   map[objectKey]string{},
		unions: map[*ir.UnionDef]string{},
		out:    b,
	}

	ctx.renderObject(f.Name+"Model", f.Root, f.Name)
}

// -----------------------------------------------------------------------------
// Core rendering
// -----------------------------------------------------------------------------

// key returns the struct key obj gets when entered at the current point of the walk.
func (ctx *renderCtx) key(obj *ir.ObjectDef) objectKey {
	return objectKey{obj: obj, level: ctx.depth.Level(obj) + 1}
}

// renderObject renders the struct for obj (children first). Map entry objects get the
// map key field first, matching the list-of-entries schema. Fields cut off by the recursion
// depth are left out, as in the schema.
func (ctx *renderCtx) renderObject(structName string, obj *ir.ObjectDef, path string) {
	k := ctx.key(obj)
	if _, ok := ctx.seen[k]; ok {
		return
	}
	ctx.seen[k] = structName
	if !ctx.depth.Enter(obj) {
		tfshape.Fail(path, "internal error: object %s entered beyond the recursion depth", obj.TypeName)
	}
	defer ctx.depth.Leave(obj)

	var fields []ir.Field
	for _, f := range ctx.idx.Fields(obj) {
		if !ctx.depth.IsCut(f.Type) {
			fields = append(fields, f)
		}
	}
	for _, f := range fields {
		ctx.renderNested(structName, f, path+"."+f.Name)
	}

	fieldDecls := make([]string, 0, len(fields)+1)
	if ctx.idx.IsMapEntry(obj) {
		fieldDecls = append(fieldDecls, decl("Name", "types.String", tfshape.MapKeyField))
	}
	for _, f := range fields {
		fieldDecls = append(fieldDecls, ctx.renderField(f, path+"."+f.Name))
	}

	ctx.emitStruct(structName, fieldDecls)
}

// renderUnion renders the struct for one element of a List{Union}: one pointer field per
// variant, exactly one of which is set.
func (ctx *renderCtx) renderUnion(structName string, u *ir.UnionDef, path string) {
	if _, ok := ctx.unions[u]; ok {
		return
	}
	ctx.unions[u] = structName

	fieldDecls := make([]string, 0, len(u.Variants))
	for _, v := range u.Variants {
		ctx.renderObject(structName+tfshape.VariantGoName(v), v.Object, path+"."+tfshape.VariantName(v))
		fieldDecls = append(fieldDecls, decl(tfshape.VariantGoName(v), "*"+ctx.objectName(v.Object, path), tfshape.VariantName(v)))
	}

	ctx.emitStruct(structName, fieldDecls)
}

func (ctx *renderCtx) emitStruct(structName string, fieldDecls []string) {
	templateData := struct {
		StructName string
		FieldDecls []string
	}{
		StructName: structName,
		FieldDecls: fieldDecls,
	}

	if err := facetModelStructTemplate.Execute(ctx.out, templateData); err != nil {
		panic(err)
	}
}

func (ctx *renderCtx) renderField(f ir.Field, path string) string {
	switch t := f.Type.(type) {
	case ir.String, ir.Bool, ir.Int, ir.Float:
		return decl(f.GoName, goType(t), f.Name)

	case ir.Object:
		return decl(f.GoName, "*"+ctx.objectName(t.Object, path), f.Name)

	case ir.List:
		switch elem := t.Elem.(type) {
		case ir.Object:
			return decl(f.GoName, "[]"+ctx.objectName(elem.Object, path), f.Name)
		case ir.Union:
			name, ok := ctx.unions[elem.Union]
			if !ok {
				tfshape.Fail(path, "internal error: union %s not registered before field rendering", elem.Union.TypeName)
			}
			return decl(f.GoName, "[]"+name, f.Name)
		}

	case ir.Map:
		switch elem := t.Elem.(type) {
		case ir.Object:
			// Map{Object} is a list of entries in Terraform; the entry struct carries the key.
			return decl(f.GoName, "[]"+ctx.objectName(elem.Object, path), f.Name)
		case ir.String, ir.Bool, ir.Int, ir.Float:
			return decl(f.GoName, "map[string]"+goType(elem), f.Name)
		}
	}

	tfshape.Fail(path, "model field type %s", tfshape.Describe(f.Type))
	return ""
}

// objectName returns the struct name of obj as a child of the object being rendered.
func (ctx *renderCtx) objectName(obj *ir.ObjectDef, path string) string {
	name, ok := ctx.seen[ctx.key(obj)]
	if !ok {
		tfshape.Fail(path, "internal error: object %s not registered before field rendering", obj.TypeName)
	}
	return name
}

// -----------------------------------------------------------------------------
// Recursion
// -----------------------------------------------------------------------------

func (ctx *renderCtx) renderNested(parent string, f ir.Field, path string) {
	switch t := f.Type.(type) {
	case ir.Object:
		ctx.renderObject(parent+f.GoName, t.Object, path)

	case ir.List:
		switch elem := t.Elem.(type) {
		case ir.Object:
			ctx.renderObject(parent+f.GoName, elem.Object, path)
		case ir.Union:
			ctx.renderUnion(parent+f.GoName, elem.Union, path)
		}

	case ir.Map:
		if o, ok := t.Elem.(ir.Object); ok {
			ctx.renderObject(parent+f.GoName, o.Object, path)
		}
	}
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func decl(goName, goType, tag string) string {
	return "\t" + goName + " " + goType + " `tfsdk:\"" + tag + "\"`"
}

func goType(t ir.Type) string {
	return "types." + t.TFKind()
}

// RenderAllFacets renders models for all facets in deterministic order.
func RenderAllFacets(
	b *strings.Builder,
	facets []ir.Facet,
) {
	sort.Slice(facets, func(i, j int) bool {
		return facets[i].Name < facets[j].Name
	})

	for _, f := range facets {
		RenderFacetModels(b, f)
	}
}

//go:embed facet_model_struct.tmpl
var facetModelStructTemplateSource string

var facetModelStructTemplate = template.Must(template.New("facet_model_struct").Parse(facetModelStructTemplateSource))

//go:embed models_file.tmpl
var modelsFileTemplateSource string

var modelsFileTemplate = template.Must(template.New("models_file").Parse(modelsFileTemplateSource))

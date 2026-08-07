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
)

type renderCtx struct {
	seen map[*ir.ObjectDef]string
	out  *strings.Builder
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

// RenderFacetModels renders all Go model structs for a facet root.
// `rootName` is e.g. ColumnLineageDatasetFacetModel.
func RenderFacetModels(
	b *strings.Builder,
	rootName string,
	root *ir.ObjectDef,
) {
	ctx := &renderCtx{
		seen: map[*ir.ObjectDef]string{},
		out:  b,
	}

	ctx.renderObject(rootName, root)
}

// -----------------------------------------------------------------------------
// Core rendering
// -----------------------------------------------------------------------------
func (ctx *renderCtx) renderObject(
	structName string,
	obj *ir.ObjectDef,
) {
	if _, ok := ctx.seen[obj]; ok {
		return
	}

	ctx.seen[obj] = structName

	for _, f := range obj.Fields {
		ctx.renderNested(structName, f)
	}

	fieldDecls := make([]string, 0, len(obj.Fields))
	for _, f := range obj.Fields {
		if decl := ctx.renderField(f); decl != "" {
			fieldDecls = append(fieldDecls, decl)
		}
	}

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

func (ctx *renderCtx) renderField(
	f ir.Field,
) string {
	switch t := f.Type.(type) {

	case ir.String:
		return "\t" + f.GoName + " types.String `tfsdk:\"" + f.Name + "\"`"

	case ir.Bool:
		return "\t" + f.GoName + " types.Bool `tfsdk:\"" + f.Name + "\"`"

	case ir.Int:
		return "\t" + f.GoName + " types.Int64 `tfsdk:\"" + f.Name + "\"`"

	case ir.Float:
		return "\t" + f.GoName + " types.Float64 `tfsdk:\"" + f.Name + "\"`"

	case ir.Map:
		switch elem := t.Elem.(type) {

		case ir.Object:
			name, ok := ctx.seen[elem.Object]
			if !ok {
				panic("internal error: map value object not registered before field rendering")
			}
			return "\t" + f.GoName + " map[string]" + name + " `tfsdk:\"" + f.Name + "\"`"

		default:
			return "\t" + f.GoName + " map[string]" + goType(elem) + " `tfsdk:\"" + f.Name + "\"`"
		}

	case ir.Object:
		// ✅ use canonical name
		name := ctx.seen[t.Object]
		return "\t" + f.GoName + " " + name + " `tfsdk:\"" + f.Name + "\"`"

	case ir.List:
		switch elem := t.Elem.(type) {

		case ir.Object:
			name, ok := ctx.seen[elem.Object]
			if !ok {
				panic("internal error: list element object not registered before field rendering")
			}
			return "\t" + f.GoName + " []" + name + " `tfsdk:\"" + f.Name + "\"`"

		default:
			return "\t" + f.GoName + " []" + goType(elem) + " `tfsdk:\"" + f.Name + "\"`"
		}

	}
	return ""
}

// -----------------------------------------------------------------------------
// Recursion
// -----------------------------------------------------------------------------
func (ctx *renderCtx) renderNested(
	parent string,
	f ir.Field,
) {
	switch t := f.Type.(type) {

	case ir.Map:
		if o, ok := t.Elem.(ir.Object); ok {
			nested := parent + f.GoName
			ctx.renderObject(nested, o.Object)
		}

	case ir.Object:
		nested := parent + f.GoName
		ctx.renderObject(nested, t.Object)

	case ir.List:
		if o, ok := t.Elem.(ir.Object); ok {
			nested := parent + f.GoName
			ctx.renderObject(nested, o.Object)
		}
	}
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func goType(t ir.Type) string {
	if kind := t.TFKind(); kind != "" {
		return "types." + kind
	}
	return "types.String"
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
		rootName := f.Name + "Model"
		RenderFacetModels(b, rootName, f.Root)
	}
}

//go:embed facet_model_struct.tmpl
var facetModelStructTemplateSource string

var facetModelStructTemplate = template.Must(template.New("facet_model_struct").Parse(facetModelStructTemplateSource))

//go:embed models_file.tmpl
var modelsFileTemplateSource string

var modelsFileTemplate = template.Must(template.New("models_file").Parse(modelsFileTemplateSource))

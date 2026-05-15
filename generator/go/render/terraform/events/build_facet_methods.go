/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package events

import (
	_ "embed"
	"fmt"
	"sort"
	"strings"
	"text/template"

	"github.com/OpenLineage/openlineage/generator/go/genutil"
	"github.com/OpenLineage/openlineage/generator/go/ir"
)

// RenderBuildFacetMethods emits facet model → OpenLineage facet conversion methods.
// All type names come from the IR (ObjectDef.TypeName), so no reflection is needed.
func RenderBuildFacetMethods(facets []ir.Facet) string {
	var methods []facetMethodEntry
	for _, f := range sortedFacets(facets) {
		if entry, ok := buildFacetMethodEntry(f); ok {
			methods = append(methods, entry)
		}
	}

	var b strings.Builder
	if err := buildFacetMethodsFileTemplate.Execute(&b, struct {
		Methods []facetMethodEntry
	}{methods}); err != nil {
		panic(err)
	}
	return b.String()
}

type facetMethodEntry struct {
	Receiver     string
	MethodName   string
	ReturnType   string
	Ctor         string
	CtorArgs     []string
	Prelude      string
	OptionalBody string
}

func buildFacetMethodEntry(f ir.Facet) (facetMethodEntry, bool) {
	isJob := strings.HasSuffix(f.Name, "JobFacet")
	if !isJob && !strings.HasSuffix(f.Name, "DatasetFacet") {
		return facetMethodEntry{}, false
	}

	receiver := f.Name + "Model"
	methodName := "BuildDatasetFacet"
	returnType := "facets.DatasetFacet"
	ctor := "facets.New" + facetBaseNameForMethod(f.Name) + "DatasetFacet"
	if isJob {
		methodName = "BuildJobFacet"
		returnType = "facets.JobFacet"
		ctor = "facets.New" + facetBaseNameForMethod(f.Name) + "JobFacet"
	}

	var prelude strings.Builder
	var optional strings.Builder
	cnt := newCounter()

	var ctorArgs []string
	for _, field := range f.Root.Fields {
		if !field.Required {
			continue
		}
		if arg := emitRequiredCtorArg(&prelude, "\t", cnt, field); arg != "" {
			ctorArgs = append(ctorArgs, arg)
		}
	}

	for _, field := range f.Root.Fields {
		if field.Required {
			continue
		}
		outField := formatFacetFieldName(field.GoName)
		if expr, ok := optionalValueExpr(field, "m."+field.GoName); ok {
			fmt.Fprintf(&optional, "\tf.%s = %s\n", outField, expr)
			continue
		}
		switch t := field.Type.(type) {
		case ir.Map:
			if _, isStr := t.Elem.(ir.String); isStr {
				emitMapStringField(&optional, outField, field.GoName)
			} else {
				emitComplexField(&optional, "\t", cnt, field)
			}
		default:
			emitComplexField(&optional, "\t", cnt, field)
		}
	}

	return facetMethodEntry{
		Receiver:     receiver,
		MethodName:   methodName,
		ReturnType:   returnType,
		Ctor:         ctor,
		CtorArgs:     ctorArgs,
		Prelude:      prelude.String(),
		OptionalBody: optional.String(),
	}, true
}

// emitRequiredCtorArg emits pre-computation code when needed and returns a constructor arg expr.
func emitRequiredCtorArg(b *strings.Builder, indent string, cnt *counter, field ir.Field) string {
	pathExpr := fmt.Sprintf("base.AtName(%q)", field.Name)
	if expr, ok := requiredValueExpr(field, "m."+field.GoName, pathExpr); ok {
		return expr
	}

	switch t := field.Type.(type) {
	case ir.List:
		if obj, ok := t.Elem.(ir.Object); ok && obj.Object.TypeName != "" {
			return emitSliceConv(b, indent, cnt, "m."+field.GoName, pathExpr, "facets."+obj.Object.TypeName, obj.Object)
		}
	case ir.Map:
		if _, isStr := t.Elem.(ir.String); isStr {
			return emitMapStringValue(b, indent, cnt, "m."+field.GoName)
		}
		if obj, ok := t.Elem.(ir.Object); ok && obj.Object.TypeName != "" {
			return emitMapObjConv(b, indent, cnt, "m."+field.GoName, pathExpr, "facets."+obj.Object.TypeName, obj.Object)
		}
	}

	fmt.Fprintf(b, "%s// TODO: unsupported required ctor arg for field %s\n", indent, field.GoName)
	return ""
}

// emitComplexField dispatches List/Map{Elem:Object} field emission using IR TypeName.
func emitComplexField(b *strings.Builder, indent string, cnt *counter, field ir.Field) {
	outField := formatFacetFieldName(field.GoName)
	switch t := field.Type.(type) {
	case ir.List:
		if obj, ok := t.Elem.(ir.Object); ok {
			typeName := obj.Object.TypeName
			if typeName != "" {
				pathExpr := fmt.Sprintf("base.AtName(%q)", field.Name)
				varName := emitSliceConv(b, indent, cnt, "m."+field.GoName, pathExpr, "facets."+typeName, obj.Object)
				if varName != "" {
					fmt.Fprintf(b, "%sf.%s = %s\n", indent, outField, varName)
				}
				return
			}
		}
	case ir.Map:
		if obj, ok := t.Elem.(ir.Object); ok {
			typeName := obj.Object.TypeName
			if typeName != "" {
				pathExpr := fmt.Sprintf("base.AtName(%q)", field.Name)
				varName := emitMapObjConv(b, indent, cnt, "m."+field.GoName, pathExpr, "facets."+typeName, obj.Object)
				if varName != "" {
					fmt.Fprintf(b, "%sf.%s = %s\n", indent, outField, varName)
				}
				return
			}
		}
	}
	fmt.Fprintf(b, "%s// TODO: no TypeName for field %s — cannot emit direct conversion\n", indent, field.GoName)
}

// ── Recursive conversion emitters ─────────────────────────────────────────────

// counter provides unique variable names and cycle detection.
type counter struct {
	n       int
	visited map[*ir.ObjectDef]bool
	names   map[string]int
}

func newCounter() *counter {
	return &counter{
		visited: map[*ir.ObjectDef]bool{},
		names:   map[string]int{},
	}
}

func (c *counter) next() int { c.n++; return c.n }

func (c *counter) nextName(base string) string {
	if base == "" {
		base = "sliceValue"
	}
	c.names[base]++
	if c.names[base] == 1 {
		return base
	}
	return fmt.Sprintf("%s%d", base, c.names[base])
}

func (c *counter) enter(obj *ir.ObjectDef) bool {
	if c.visited[obj] {
		return false
	}
	c.visited[obj] = true
	return true
}

func (c *counter) leave(obj *ir.ObjectDef) { delete(c.visited, obj) }

// emitSliceConv emits a loop building []facets.ElemType from a model slice.
func emitSliceConv(
	b *strings.Builder,
	indent string,
	cnt *counter,
	srcExpr, outerPath, olElemName string,
	irObj *ir.ObjectDef,
) string {
	if !cnt.enter(irObj) {
		return ""
	}
	defer cnt.leave(irObj)

	id := cnt.next()
	sliceVar := cnt.nextName(localVarNameForType(olElemName))
	itemVar := fmt.Sprintf("_item%d", id)
	idxVar := fmt.Sprintf("_i%d", id)
	pathVar := fmt.Sprintf("_p%d", id)
	inner := indent + "\t"
	innermost := indent + "\t\t"

	fmt.Fprintf(b, "%s%s := make([]%s, 0, len(%s))\n", indent, sliceVar, olElemName, srcExpr)
	fmt.Fprintf(b, "%sfor %s, %s := range %s {\n", indent, idxVar, itemVar, srcExpr)
	fmt.Fprintf(b, "%s%s := %s.AtListIndex(%s)\n", inner, pathVar, outerPath, idxVar)

	precomputed := map[string]string{}
	for _, field := range irObj.Fields {
		preVarName := emitNestedField(b, inner, cnt, field, itemVar, pathVar)
		if preVarName != "" {
			precomputed[field.GoName] = preVarName
		}
	}

	fmt.Fprintf(b, "%s%s = append(%s, %s{\n", inner, sliceVar, sliceVar, olElemName)
	for _, field := range irObj.Fields {
		outField := formatFacetFieldName(field.GoName)
		if varName, ok := precomputed[field.GoName]; ok {
			fmt.Fprintf(b, "%s%s: %s,\n", innermost, outField, varName)
			continue
		}
		emitStructField(b, innermost, field, outField, itemVar+"."+field.GoName, fmt.Sprintf("%s.AtName(%q)", pathVar, field.Name))
	}
	fmt.Fprintf(b, "%s})\n", inner)
	fmt.Fprintf(b, "%s}\n", indent)

	return sliceVar
}

// emitMapObjConv emits a loop building map[string]facets.ElemType from a model map.
func emitMapObjConv(
	b *strings.Builder,
	indent string,
	cnt *counter,
	srcExpr, outerPath, olElemName string,
	irObj *ir.ObjectDef,
) string {
	if !cnt.enter(irObj) {
		return ""
	}
	defer cnt.leave(irObj)

	id := cnt.next()
	mapVar := fmt.Sprintf("_map%d", id)
	itemVar := fmt.Sprintf("_item%d", id)
	keyVar := fmt.Sprintf("_k%d", id)
	pathVar := fmt.Sprintf("_p%d", id)
	inner := indent + "\t"
	innermost := indent + "\t\t"

	fmt.Fprintf(b, "%s%s := make(map[string]%s, len(%s))\n", indent, mapVar, olElemName, srcExpr)
	fmt.Fprintf(b, "%sfor %s, %s := range %s {\n", indent, keyVar, itemVar, srcExpr)
	fmt.Fprintf(b, "%s%s := %s.AtMapKey(%s)\n", inner, pathVar, outerPath, keyVar)

	precomputed := map[string]string{}
	for _, field := range irObj.Fields {
		preVarName := emitNestedField(b, inner, cnt, field, itemVar, pathVar)
		if preVarName != "" {
			precomputed[field.GoName] = preVarName
		}
	}

	fmt.Fprintf(b, "%s%s[%s] = %s{\n", inner, mapVar, keyVar, olElemName)
	for _, field := range irObj.Fields {
		outField := formatFacetFieldName(field.GoName)
		if varName, ok := precomputed[field.GoName]; ok {
			fmt.Fprintf(b, "%s%s: %s,\n", innermost, outField, varName)
			continue
		}
		emitStructField(b, innermost, field, outField, itemVar+"."+field.GoName, fmt.Sprintf("%s.AtName(%q)", pathVar, field.Name))
	}
	fmt.Fprintf(b, "%s}\n", inner)
	fmt.Fprintf(b, "%s}\n", indent)

	return mapVar
}

// emitNestedField pre-computes a List or Map{Elem:Object} field inside a loop body.
// Returns the emitted variable name, or "" for scalar fields.
func emitNestedField(
	b *strings.Builder,
	indent string,
	cnt *counter,
	field ir.Field,
	itemVar, pathVar string,
) string {
	switch ft := field.Type.(type) {
	case ir.List:
		if obj, ok := ft.Elem.(ir.Object); ok && obj.Object.TypeName != "" {
			pathExpr := fmt.Sprintf("%s.AtName(%q)", pathVar, field.Name)
			v := emitSliceConv(b, indent, cnt,
				itemVar+"."+field.GoName, pathExpr,
				"facets."+obj.Object.TypeName, obj.Object)
			return v
		}
	case ir.Map:
		if obj, ok := ft.Elem.(ir.Object); ok && obj.Object.TypeName != "" {
			pathExpr := fmt.Sprintf("%s.AtName(%q)", pathVar, field.Name)
			v := emitMapObjConv(b, indent, cnt,
				itemVar+"."+field.GoName, pathExpr,
				"facets."+obj.Object.TypeName, obj.Object)
			return v
		}
	}
	return ""
}

// emitStructField emits a single field assignment inside a struct literal for scalar types.
func emitStructField(b *strings.Builder, indent string, field ir.Field, outField, srcExpr, pathExpr string) {
	if field.Required {
		if expr, ok := requiredValueExpr(field, srcExpr, pathExpr); ok {
			fmt.Fprintf(b, "%s%s: %s,\n", indent, outField, expr)
		}
		return
	}
	if expr, ok := optionalValueExpr(field, srcExpr); ok {
		fmt.Fprintf(b, "%s%s: %s,\n", indent, outField, expr)
	}
}

func requiredValueExpr(field ir.Field, srcExpr, pathExpr string) (string, bool) {
	if kind := field.Type.TFKind(); kind != "" {
		return fmt.Sprintf("required%sValue(diags, %s, %s)", kind, srcExpr, pathExpr), true
	}
	return "", false
}

func optionalValueExpr(field ir.Field, srcExpr string) (string, bool) {
	if kind := field.Type.TFKind(); kind != "" {
		return fmt.Sprintf("optional%sValue(%s)", kind, srcExpr), true
	}
	return "", false
}

// emitMapStringValue emits a map[string]types.String -> map[string]string conversion and returns the var name.
func emitMapStringValue(b *strings.Builder, indent string, cnt *counter, srcExpr string) string {
	id := cnt.next()
	mapVar := fmt.Sprintf("_m%d", id)
	keyVar := fmt.Sprintf("_k%d", id)
	valVar := fmt.Sprintf("_v%d", id)

	fmt.Fprintf(b, "%s%s := make(map[string]string, len(%s))\n", indent, mapVar, srcExpr)
	fmt.Fprintf(b, "%sfor %s, %s := range %s {\n", indent, keyVar, valVar, srcExpr)
	fmt.Fprintf(b, "%s\tif !%s.IsNull() && !%s.IsUnknown() {\n", indent, valVar, valVar)
	fmt.Fprintf(b, "%s\t\t%s[%s] = %s.ValueString()\n", indent, mapVar, keyVar, valVar)
	fmt.Fprintf(b, "%s\t}\n", indent)
	fmt.Fprintf(b, "%s}\n", indent)

	return mapVar
}

// emitMapStringField emits a null-safe map[string]types.String → map[string]string assignment.
func emitMapStringField(b *strings.Builder, facetField, modelField string) {
	varName := "_m" + facetField
	fmt.Fprintf(b, "\tif len(m.%s) > 0 {\n", modelField)
	fmt.Fprintf(b, "\t\t%s := make(map[string]string, len(m.%s))\n", varName, modelField)
	fmt.Fprintf(b, "\t\tfor k, v := range m.%s {\n", modelField)
	fmt.Fprintf(b, "\t\t\tif !v.IsNull() && !v.IsUnknown() {\n")
	fmt.Fprintf(b, "\t\t\t\t%s[k] = v.ValueString()\n", varName)
	fmt.Fprintf(b, "\t\t\t}\n")
	fmt.Fprintf(b, "\t\t}\n")
	fmt.Fprintf(b, "\t\tif len(%s) > 0 {\n", varName)
	fmt.Fprintf(b, "\t\t\tf.%s = %s\n", facetField, varName)
	fmt.Fprintf(b, "\t\t}\n")
	fmt.Fprintf(b, "\t}\n")
}

// ── Sorting & naming helpers ──────────────────────────────────────────────────

func sortedFacets(facets []ir.Facet) []ir.Facet {
	out := make([]ir.Facet, len(facets))
	copy(out, facets)
	sort.Slice(out, func(i, j int) bool {
		if string(out[i].Kind) == string(out[j].Kind) {
			return out[i].Name < out[j].Name
		}
		return string(out[i].Kind) < string(out[j].Kind)
	})
	return out
}

func facetBaseNameForMethod(facetName string) string {
	base := strings.TrimSuffix(facetName, "Facet")
	base = strings.TrimSuffix(base, "Job")
	base = strings.TrimSuffix(base, "Dataset")
	return base
}

func localVarNameForType(typeName string) string {
	short := typeName
	if i := strings.LastIndex(typeName, "."); i >= 0 && i+1 < len(typeName) {
		short = typeName[i+1:]
	}
	if short == "" {
		return "sliceValue"
	}
	return strings.ToLower(short[:1]) + short[1:]
}

// formatFacetFieldName maps a model GoName to the OL facet struct field name.
func formatFacetFieldName(name string) string {
	return genutil.FormatGoFieldName(name)
}

// Ensure internal package is used.
var _ = genutil.ToSnake

//go:embed build_facet_methods_file.tmpl
var buildFacetMethodsFileTemplateSource string

var buildFacetMethodsFileTemplate = template.Must(template.New("build_facet_methods_file").Parse(buildFacetMethodsFileTemplateSource))

/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package events

import (
	_ "embed"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"text/template"

	"github.com/OpenLineage/openlineage/generator/go/genutil"
	"github.com/OpenLineage/openlineage/generator/go/ir"
	"github.com/OpenLineage/openlineage/generator/go/render/terraform/tfshape"
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

	e := &emitter{idx: tfshape.NewIndex(f), cnt: newCounter(), facet: f.Name}
	e.cnt.depth.Enter(f.Root) // count the root like the schema and model renderers do
	var prelude strings.Builder
	var optional strings.Builder

	fields := e.idx.Fields(f.Root)

	var ctorArgs []string
	for _, field := range fields {
		if !field.Required {
			continue
		}
		arg := e.value(&prelude, "\t", field, "m."+field.GoName, fmt.Sprintf("base.AtName(%q)", field.Name), f.Name+"."+field.Name)
		if arg == "" {
			tfshape.Fail(f.Name+"."+field.Name, "required field that can't be converted")
		}
		ctorArgs = append(ctorArgs, arg)
	}

	for _, field := range fields {
		if field.Required {
			continue
		}
		outField := formatFacetFieldName(field.GoName)
		if m, ok := field.Type.(ir.Map); ok {
			if _, isStr := m.Elem.(ir.String); isStr {
				emitMapStringField(&optional, outField, field.GoName)
				continue
			}
		}
		expr := e.value(&optional, "\t", field, "m."+field.GoName, fmt.Sprintf("base.AtName(%q)", field.Name), f.Name+"."+field.Name)
		if expr != "" {
			fmt.Fprintf(&optional, "\tf.%s = %s\n", outField, expr)
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

// ── Recursive conversion emitter ─────────────────────────────────────────────

// emitter converts Terraform model values to OpenLineage client values for one facet.
type emitter struct {
	idx   *tfshape.Index
	cnt   *counter
	facet string
}

// value writes any statements needed to b and returns a Go expression of the client type
// for field f, read from the model expression src, with attribute path expression p.
// irPath is the facet/field path used in generator error messages.
// It returns "" only when the field is cut off at tfshape.MaxRecursionDepth.
func (e *emitter) value(b *strings.Builder, indent string, f ir.Field, src, p, irPath string) string {
	switch t := f.Type.(type) {
	case ir.String, ir.Bool, ir.Int, ir.Float:
		if f.Required {
			return fmt.Sprintf("Required%sValue(diags, %s, %s)", t.TFKind(), src, p)
		}
		return fmt.Sprintf("Optional%sValue(%s)", t.TFKind(), src)

	case ir.Object:
		return e.object(b, indent, t.Object, f.Required, src, p, irPath)

	case ir.List:
		switch elem := t.Elem.(type) {
		case ir.Object:
			return e.objectList(b, indent, elem.Object, src, p, irPath)
		case ir.Union:
			return e.unionList(b, indent, elem.Union, src, p, irPath)
		}

	case ir.Map:
		switch elem := t.Elem.(type) {
		case ir.String:
			return emitMapStringValue(b, indent, e.cnt, src)
		case ir.Object:
			return e.objectMap(b, indent, elem.Object, src, p, irPath)
		}
	}

	tfshape.Fail(irPath, "facet builder field type %s", tfshape.Describe(f.Type))
	return ""
}

// literal returns a client struct literal for obj read from src (a model struct or pointer).
// Nested lists/maps/objects are computed into variables first. For a union variant,
// discriminatorValue is the constant written into the discriminator field.
func (e *emitter) literal(b *strings.Builder, indent string, obj *ir.ObjectDef, src, p, irPath, discriminatorValue string) string {
	var lit strings.Builder
	fmt.Fprintf(&lit, "facets.%s{\n", obj.TypeName)
	if d, ok := e.idx.Discriminator(obj); ok {
		enum, _ := d.Type.(ir.Enum)
		fmt.Fprintf(&lit, "%s\t%s: facets.%s(%q),\n", indent, formatFacetFieldName(d.GoName), enum.Enum.TypeName, discriminatorValue)
	}
	for _, field := range e.idx.Fields(obj) {
		expr := e.value(b, indent, field, src+"."+field.GoName, fmt.Sprintf("%s.AtName(%q)", p, field.Name), irPath+"."+field.Name)
		if expr == "" {
			continue
		}
		fmt.Fprintf(&lit, "%s\t%s: %s,\n", indent, formatFacetFieldName(field.GoName), expr)
	}
	fmt.Fprintf(&lit, "%s}", indent)
	return lit.String()
}

// object converts a single nested block (a pointer in the model).
func (e *emitter) object(b *strings.Builder, indent string, obj *ir.ObjectDef, required bool, src, p, irPath string) string {
	if !e.cnt.depth.Enter(obj) {
		return "" // cut off at tfshape.MaxRecursionDepth
	}
	defer e.cnt.depth.Leave(obj)

	v := fmt.Sprintf("_o%d", e.cnt.next())
	inner := indent + "\t"
	var body strings.Builder
	lit := e.literal(&body, inner, obj, src, p, irPath, "")

	if required {
		fmt.Fprintf(b, "%svar %s facets.%s\n", indent, v, obj.TypeName)
	} else {
		fmt.Fprintf(b, "%svar %s *facets.%s\n", indent, v, obj.TypeName)
		lit = "&" + lit
	}
	fmt.Fprintf(b, "%sif %s != nil {\n", indent, src)
	b.WriteString(body.String())
	fmt.Fprintf(b, "%s%s = %s\n", inner, v, lit)
	if required {
		fmt.Fprintf(b, "%s} else {\n", indent)
		fmt.Fprintf(b, "%sdiags.AddAttributeError(%s, \"Missing required block\", \"This block is required.\")\n", inner, p)
	}
	fmt.Fprintf(b, "%s}\n", indent)
	return v
}

// loop holds the names used by one generated for-loop.
type loop struct {
	idx, item, path string
}

func (e *emitter) newLoop() loop {
	id := e.cnt.next()
	return loop{
		idx:  fmt.Sprintf("_i%d", id),
		item: fmt.Sprintf("_item%d", id),
		path: fmt.Sprintf("_p%d", id),
	}
}

// writeLoop writes `for ... range src { body }`, declaring the index, item and path
// variables only when body uses them.
func writeLoop(b *strings.Builder, indent string, l loop, src, p, body string) {
	usesPath := usesIdent(body, l.path)
	usesItem := usesIdent(body, l.item)
	idx, item := "_", "_"
	if usesPath {
		idx = l.idx
	}
	if usesItem {
		item = l.item
	}
	switch {
	case !usesPath && !usesItem:
		fmt.Fprintf(b, "%sfor range %s {\n", indent, src)
	case !usesItem:
		fmt.Fprintf(b, "%sfor %s := range %s {\n", indent, idx, src)
	default:
		fmt.Fprintf(b, "%sfor %s, %s := range %s {\n", indent, idx, item, src)
	}
	if usesPath {
		fmt.Fprintf(b, "%s\t%s := %s.AtListIndex(%s)\n", indent, l.path, p, l.idx)
	}
	b.WriteString(body)
	fmt.Fprintf(b, "%s}\n", indent)
}

// objectList converts a List{Object}.
func (e *emitter) objectList(b *strings.Builder, indent string, obj *ir.ObjectDef, src, p, irPath string) string {
	if !e.cnt.depth.Enter(obj) {
		return "" // cut off at tfshape.MaxRecursionDepth
	}
	defer e.cnt.depth.Leave(obj)

	clientType := "facets." + obj.TypeName
	sliceVar := e.cnt.nextName(localVarNameForType(clientType))
	l := e.newLoop()
	inner := indent + "\t"

	var body strings.Builder
	lit := e.literal(&body, inner, obj, l.item, l.path, irPath, "")
	fmt.Fprintf(&body, "%s%s = append(%s, %s)\n", inner, sliceVar, sliceVar, lit)

	fmt.Fprintf(b, "%s%s := make([]%s, 0, len(%s))\n", indent, sliceVar, clientType, src)
	writeLoop(b, indent, l, src, p, body.String())
	return sliceVar
}

// unionList converts a List{Union}: each element has one pointer per variant, exactly one set.
func (e *emitter) unionList(b *strings.Builder, indent string, u *ir.UnionDef, src, p, irPath string) string {
	rawType := "facets.Raw" + u.TypeName
	sliceVar := e.cnt.nextName(localVarNameForType(rawType))
	l := e.newLoop()
	inner := indent + "\t"
	caseIndent := inner + "\t"

	var body strings.Builder
	fmt.Fprintf(&body, "%sswitch {\n", inner)
	names := make([]string, 0, len(u.Variants))
	for _, v := range u.Variants {
		name := tfshape.VariantName(v)
		names = append(names, name)
		variantSrc := l.item + "." + tfshape.VariantGoName(v)
		fmt.Fprintf(&body, "%scase %s != nil:\n", inner, variantSrc)
		if e.cnt.depth.Level(v.Object) > 0 || !e.cnt.depth.Enter(v.Object) {
			tfshape.Fail(irPath+"."+name, "recursive union variant %s", v.Object.TypeName)
		}
		lit := e.literal(&body, caseIndent, v.Object, variantSrc, fmt.Sprintf("%s.AtName(%q)", l.path, name), irPath+"."+name, v.DiscriminatorValue)
		e.cnt.depth.Leave(v.Object)
		fmt.Fprintf(&body, "%s%s = append(%s, %s{V: %s})\n", caseIndent, sliceVar, sliceVar, rawType, lit)
	}
	sort.Strings(names)
	fmt.Fprintf(&body, "%sdefault:\n", inner)
	fmt.Fprintf(&body, "%sdiags.AddAttributeError(%s, \"Invalid block\", %q)\n", caseIndent, l.path, "Exactly one of "+strings.Join(names, ", ")+" must be set.")
	fmt.Fprintf(&body, "%s}\n", inner)

	fmt.Fprintf(b, "%s%s := make([]%s, 0, len(%s))\n", indent, sliceVar, rawType, src)
	writeLoop(b, indent, l, src, p, body.String())
	return sliceVar
}

// objectMap converts a Map{Object}, which Terraform holds as a list of entries keyed by "name".
func (e *emitter) objectMap(b *strings.Builder, indent string, obj *ir.ObjectDef, src, p, irPath string) string {
	if e.cnt.depth.Level(obj) > 0 || !e.cnt.depth.Enter(obj) {
		tfshape.Fail(irPath, "recursive map entry object %s", obj.TypeName)
	}
	defer e.cnt.depth.Leave(obj)

	clientType := "facets." + obj.TypeName
	id := e.cnt.next()
	mapVar := fmt.Sprintf("_map%d", id)
	keyVar := fmt.Sprintf("_k%d", id)
	l := e.newLoop()
	inner := indent + "\t"
	keyPath := fmt.Sprintf("%s.AtName(%q)", l.path, tfshape.MapKeyField)

	var body strings.Builder
	fmt.Fprintf(&body, "%s%s := RequiredStringValue(diags, %s.Name, %s)\n", inner, keyVar, l.item, keyPath)
	fmt.Fprintf(&body, "%sif _, dup := %s[%s]; dup {\n", inner, mapVar, keyVar)
	fmt.Fprintf(&body, "%s\tdiags.AddAttributeError(%s, \"Duplicate name\", \"Name \\\"\"+%s+\"\\\" is used more than once.\")\n", inner, keyPath, keyVar)
	fmt.Fprintf(&body, "%s}\n", inner)
	lit := e.literal(&body, inner, obj, l.item, l.path, irPath, "")
	fmt.Fprintf(&body, "%s%s[%s] = %s\n", inner, mapVar, keyVar, lit)

	fmt.Fprintf(b, "%s%s := make(map[string]%s, len(%s))\n", indent, mapVar, clientType, src)
	writeLoop(b, indent, l, src, p, body.String())
	return mapVar
}

// usesIdent reports whether the generated code in body references the identifier name.
func usesIdent(body, name string) bool {
	return regexp.MustCompile(`\b` + regexp.QuoteMeta(name) + `\b`).MatchString(body)
}

// ── Counter ──────────────────────────────────────────────────────────────────

// counter provides unique variable names and tracks the recursion depth.
type counter struct {
	n     int
	depth tfshape.Depth
	names map[string]int
}

func newCounter() *counter {
	return &counter{
		depth: tfshape.Depth{},
		names: map[string]int{},
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

// ── Map[string]string helpers ────────────────────────────────────────────────

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

//go:embed build_facet_methods_file.tmpl
var buildFacetMethodsFileTemplateSource string

var buildFacetMethodsFileTemplate = template.Must(template.New("build_facet_methods_file").Parse(buildFacetMethodsFileTemplateSource))

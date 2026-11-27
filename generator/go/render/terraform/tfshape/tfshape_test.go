/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package tfshape

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/OpenLineage/openlineage/generator/go/ir"
)

func field(name string, t ir.Type, required bool) ir.Field {
	return ir.Field{Name: name, GoName: strings.ToUpper(name[:1]) + name[1:], JSONName: name, Type: t, Required: required}
}

func obj(name string, fields ...ir.Field) *ir.ObjectDef {
	return &ir.ObjectDef{TypeName: name, Fields: fields, Required: map[string]bool{}, DependentRequired: map[string][]string{}}
}

func facet(root *ir.ObjectDef) ir.Facet {
	return ir.Facet{Name: "TestJobFacet", Root: root}
}

func enumType() ir.Enum { return ir.Enum{Enum: &ir.EnumDef{TypeName: "Kind"}} }

func union(name, discriminator string, variants ...ir.Variant) ir.Union {
	return ir.Union{Union: &ir.UnionDef{TypeName: name, DiscriminatorField: discriminator, Variants: variants}}
}

func variant(value string, o *ir.ObjectDef) ir.Variant {
	return ir.Variant{DiscriminatorValue: value, Object: o}
}

// expectUnsupported runs NewIndex and checks it fails at path with a message containing what.
func expectUnsupported(t *testing.T, f ir.Facet, path, what string) {
	t.Helper()
	defer func() {
		t.Helper()
		r := recover()
		err, ok := r.(error)
		var unsupported UnsupportedError
		if !ok || !errors.As(err, &unsupported) {
			t.Fatalf("NewIndex: got %v, want UnsupportedError", r)
		}
		if unsupported.Path != path || !strings.Contains(unsupported.What, what) {
			t.Fatalf("NewIndex failed at %q with %q, want %q containing %q", unsupported.Path, unsupported.What, path, what)
		}
	}()
	NewIndex(f)
}

func TestNewIndexRejectsUnsupportedShapes(t *testing.T) {
	entry := obj("Entry", field("value", ir.String{}, false))
	plain := obj("Plain", field("x", ir.String{}, false))
	cases := []struct {
		name, path, what string
		root             *ir.ObjectDef
	}{
		{"list of scalars", "TestJobFacet.items", "list of", obj("Root", field("items", ir.List{Elem: ir.String{}}, false))},
		{"date-time", "TestJobFacet.at", "date-time", obj("Root", field("at", ir.DateTime{}, false))},
		{"untyped", "TestJobFacet.any", "untyped value", obj("Root", field("any", ir.Any{}, false))},
		{"enum", "TestJobFacet.kind", "enum", obj("Root", field("kind", enumType(), false))},
		{"union without discriminator", "TestJobFacet.entries", "without a discriminator",
			obj("Root", field("entries", ir.List{Elem: union("U", "", variant("A", obj("A")))}, false))},
		{"variant name clash", "TestJobFacet.entries", "two variants named",
			obj("Root", field("entries", ir.List{Elem: union("U", "type",
				variant("A", obj("A1", field("type", enumType(), true))),
				variant("a", obj("A2", field("type", enumType(), true))))}, false))},
		{"discriminator not an enum", "TestJobFacet.entries.a.type", "not an enum",
			obj("Root", field("entries", ir.List{Elem: union("U", "type", variant("A", obj("A", field("type", ir.String{}, true))))}, false))},
		{"map entry with own name", "TestJobFacet.m.name", "own \"name\" field",
			obj("Root", field("m", ir.Map{Elem: ir.Object{Object: obj("E", field("name", ir.String{}, false))}}, false))},
		{"object as map entry and plain", "TestJobFacet.p", "both as map entry",
			obj("Root", field("m", ir.Map{Elem: ir.Object{Object: entry}}, false), field("p", ir.Object{Object: entry}, false))},
		{"map of lists", "TestJobFacet.m", "map of list",
			obj("Root", field("m", ir.Map{Elem: ir.List{Elem: ir.Object{Object: plain}}}, false))},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			expectUnsupported(t, facet(c.root), c.path, c.what)
		})
	}
}

func TestDiscriminatorNotExposed(t *testing.T) {
	a := obj("A", field("name", ir.String{}, true), field("type", enumType(), true))
	a.Required = map[string]bool{"name": true, "type": true}
	root := obj("Root", field("entries", ir.List{Elem: union("U", "type", variant("A", a))}, false))
	x := NewIndex(facet(root))

	var names []string
	for _, f := range x.Fields(a) {
		names = append(names, f.Name)
	}
	if !reflect.DeepEqual(names, []string{"name"}) {
		t.Errorf("Fields = %v, want [name]", names)
	}
	if got := x.RequiredKeys(a); !reflect.DeepEqual(got, []string{"name"}) {
		t.Errorf("RequiredKeys = %v, want [name]", got)
	}
	if d, ok := x.Discriminator(a); !ok || d.Name != "type" {
		t.Errorf("Discriminator = %v, %v; want type", d, ok)
	}
}

// A recursive object is entered MaxRecursionDepth times; the next field is cut.
func TestExcludedFieldsLeftOut(t *testing.T) {
	saved := ExcludedFields
	t.Cleanup(func() { ExcludedFields = saved })
	ExcludedFields = []string{"Job.facets"}

	job := obj("Job",
		field("name", ir.String{}, true),
		field("facets", ir.Map{Elem: ir.Any{}}, false), // unsupported shape, but excluded
	)
	x := NewIndex(facet(obj("Root", field("job", ir.Object{Object: job}, true))))
	var names []string
	for _, f := range x.Fields(job) {
		names = append(names, f.Name)
	}
	if !reflect.DeepEqual(names, []string{"name"}) {
		t.Errorf("Fields = %v, want [name]", names)
	}
	if got := x.ExcludedFound(); !reflect.DeepEqual(got, []string{"Job.facets"}) {
		t.Errorf("ExcludedFound = %v, want [Job.facets]", got)
	}

	required := obj("Job", field("facets", ir.String{}, true))
	expectUnsupported(t, facet(obj("Root", field("job", ir.Object{Object: required}, false))),
		"TestJobFacet.job.facets", "excluded field that is required")
}

func TestRecursionCutAtMaxDepth(t *testing.T) {
	node := obj("Node", field("name", ir.String{}, true))
	node.Fields = append(node.Fields, field("children", ir.List{Elem: ir.Object{Object: node}}, false))
	root := obj("Root", field("children", ir.List{Elem: ir.Object{Object: node}}, false))
	NewIndex(facet(root)) // recursion is supported

	d := Depth{}
	children := ir.List{Elem: ir.Object{Object: node}}
	levels := 0
	for !d.IsCut(children) {
		if !d.Enter(node) {
			t.Fatal("Enter refused before IsCut reported the cut")
		}
		levels++
	}
	if levels != MaxRecursionDepth {
		t.Fatalf("unrolled %d levels, want %d", levels, MaxRecursionDepth)
	}
	if d.Enter(node) {
		t.Fatal("Enter accepted a level beyond MaxRecursionDepth")
	}
	d.Leave(node)
	if d.IsCut(children) || d.Level(node) != MaxRecursionDepth-1 {
		t.Fatalf("after Leave: cut=%v level=%d", d.IsCut(children), d.Level(node))
	}
}

func TestRecursionRequiredFieldRejected(t *testing.T) {
	node := obj("Node")
	node.Fields = []ir.Field{field("child", ir.Object{Object: node}, true)}
	expectUnsupported(t, facet(obj("Root", field("child", ir.Object{Object: node}, false))),
		"TestJobFacet.child"+strings.Repeat(".child", MaxRecursionDepth), "required recursive field")
}

func TestDependentRequired(t *testing.T) {
	o := obj("O",
		ir.Field{Name: "run_id", JSONName: "runId", Type: ir.String{}},
		ir.Field{Name: "name", JSONName: "name", Type: ir.String{}},
		ir.Field{Name: "namespace", JSONName: "namespace", Type: ir.String{}})
	o.DependentRequired = map[string][]string{"runId": {"namespace", "name"}, "name": {"namespace"}}
	x := NewIndex(facet(obj("Root", field("o", ir.Object{Object: o}, false))))
	want := map[string][]string{"run_id": {"name", "namespace"}, "name": {"namespace"}}
	if got := x.DependentRequired(o); !reflect.DeepEqual(got, want) {
		t.Fatalf("DependentRequired = %v, want %v", got, want)
	}

	bad := obj("B", field("count", ir.Int{}, false), field("name", ir.String{}, false))
	bad.DependentRequired = map[string][]string{"count": {"name"}}
	expectUnsupported(t, facet(obj("Root", field("b", ir.Object{Object: bad}, false))), "TestJobFacet.b.count", "only strings")
}

func TestFacetKey(t *testing.T) {
	for in, want := range map[string]string{
		"SQLJobFacet":                "sql",
		"DatasetVersionDatasetFacet": "version",
		"JobTypeJobFacet":            "job_type",
		"SourceCodeLocationJobFacet": "source_code_location",
		"ColumnLineageDatasetFacet":  "column_lineage",
		"FabricOneLakeDatasetFacet":  "fabric_one_lake",
		"SymlinksDatasetFacet":       "symlinks",
	} {
		if got := FacetKey(in); got != want {
			t.Errorf("FacetKey(%q) = %q, want %q", in, got, want)
		}
	}
}

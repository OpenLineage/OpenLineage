/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

// Package tfshape holds the rules that decide how IR types map to Terraform shapes.
// The model, schema descriptor and facet builder renderers all use it, so they agree on
// field names, union variant blocks and map entries, and so an unsupported shape stops the
// generator instead of being skipped silently.
//
// Mapping:
//   - scalars (String, Bool, Int, Float) and Map{scalar} → attributes
//   - Object → single nested block (pointer in the model)
//   - List{Object} → list nested block
//   - List{Union} → list nested block whose element has one single nested block per variant,
//     named after the lowercase discriminator value; the discriminator itself is not exposed
//   - Map{Object} → list nested block of entries with a "name" attribute holding the map key
//
// Fields listed in ExcludedFields are left out of every renderer.
//
// Recursive objects (SchemaDatasetFacet.fields) are unrolled to MaxRecursionDepth levels: a
// field whose object is already that many times on the current path is left out. All
// renderers count with Depth, so schema, model and builder unroll identically.
package tfshape

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"unicode"

	"github.com/OpenLineage/openlineage/generator/go/ir"
)

// MapKeyField is the attribute that holds the map key in a Map{Object} entry.
const MapKeyField = "name"

// MaxRecursionDepth is how many times one object may appear on a path from the facet root.
// Terraform schemas can't express cycles, so recursive types are unrolled to this depth.
const MaxRecursionDepth = 3

// ExcludedFields are object fields left out of Terraform, as "<object type>.<JSON name>".
// Each must be optional; the coverage test fails on entries no longer in the spec.
var ExcludedFields = []string{
	// Arbitrary job facets forwarded from the parent job: HCL has no shape for a map of any
	// facet, and the parent's own event carries them anyway.
	"ParentJobFacetJob.facets",
}

// ExcludedFieldKey is the ExcludedFields entry that would exclude f of obj.
func ExcludedFieldKey(obj *ir.ObjectDef, f ir.Field) string {
	return obj.TypeName + "." + f.JSONName
}

func isExcludedField(obj *ir.ObjectDef, f ir.Field) bool {
	return slices.Contains(ExcludedFields, ExcludedFieldKey(obj, f))
}

// Depth counts how often each object is on the current path of a walk.
type Depth map[*ir.ObjectDef]int

// Enter records obj on the path; false (and nothing recorded) if that would exceed
// MaxRecursionDepth.
func (d Depth) Enter(obj *ir.ObjectDef) bool {
	if d[obj] >= MaxRecursionDepth {
		return false
	}
	d[obj]++
	return true
}

// Leave undoes Enter.
func (d Depth) Leave(obj *ir.ObjectDef) {
	d[obj]--
}

// Level is how many times obj is on the current path.
func (d Depth) Level(obj *ir.ObjectDef) int {
	return d[obj]
}

// IsCut reports whether a field of type t is left out at this point of the walk because its
// object is already MaxRecursionDepth times on the path.
func (d Depth) IsCut(t ir.Type) bool {
	obj := ObjectOf(t)
	return obj != nil && d[obj] >= MaxRecursionDepth
}

// ObjectOf returns the object behind an Object, List{Object} or Map{Object} type, or nil.
func ObjectOf(t ir.Type) *ir.ObjectDef {
	switch tt := t.(type) {
	case ir.Object:
		return tt.Object
	case ir.List:
		if o, ok := tt.Elem.(ir.Object); ok {
			return o.Object
		}
	case ir.Map:
		if o, ok := tt.Elem.(ir.Object); ok {
			return o.Object
		}
	}
	return nil
}

// UnsupportedError is raised (as a panic value) when a facet contains a shape the Terraform
// renderers can't represent. main recovers it and exits with the message.
type UnsupportedError struct {
	Path string
	What string
}

func (e UnsupportedError) Error() string {
	return fmt.Sprintf("terraform generator: unsupported %s at %s", e.What, e.Path)
}

// Fail panics with an UnsupportedError.
func Fail(path, format string, args ...any) {
	panic(UnsupportedError{Path: path, What: fmt.Sprintf(format, args...)})
}

type role int

const (
	roleObject role = iota + 1
	roleMapEntry
	roleVariant
)

// Index records, for one facet, which objects are union variants (and their discriminator)
// and which are map entries. NewIndex validates the whole facet up front.
type Index struct {
	roles         map[*ir.ObjectDef]role
	discriminator map[*ir.ObjectDef]string // variant object → discriminator JSON name
	excluded      map[string]bool          // ExcludedFields entries found in this facet
}

// NewIndex walks the facet and fails on any shape the Terraform renderers don't support.
func NewIndex(f ir.Facet) *Index {
	x := &Index{
		roles:         map[*ir.ObjectDef]role{},
		discriminator: map[*ir.ObjectDef]string{},
		excluded:      map[string]bool{},
	}
	x.walkObject(f.Name, f.Root, roleObject, "", Depth{})
	return x
}

func (x *Index) setRole(path string, obj *ir.ObjectDef, r role) {
	if prev, ok := x.roles[obj]; ok && prev != r {
		Fail(path, "object %s used both as map entry/union variant and as a plain object", obj.TypeName)
	}
	x.roles[obj] = r
}

func (x *Index) walkObject(path string, obj *ir.ObjectDef, r role, discriminator string, depth Depth) {
	x.setRole(path, obj, r)
	if discriminator != "" {
		x.discriminator[obj] = discriminator
	}
	if r != roleObject && depth.Level(obj) > 0 {
		Fail(path, "recursive map entry or union variant %s", obj.TypeName)
	}
	if !depth.Enter(obj) {
		return // recursive type, unrolled to MaxRecursionDepth
	}
	defer depth.Leave(obj)

	for _, f := range obj.Fields {
		fp := path + "." + f.Name
		if isExcludedField(obj, f) {
			if f.Required {
				Fail(fp, "excluded field that is required")
			}
			x.excluded[ExcludedFieldKey(obj, f)] = true
			continue
		}
		if discriminator != "" && f.JSONName == discriminator {
			if _, ok := f.Type.(ir.Enum); !ok {
				Fail(fp, "union discriminator that is not an enum")
			}
			continue
		}
		if r == roleMapEntry && f.Name == MapKeyField {
			Fail(fp, "map entry object with its own %q field (clashes with the map key attribute)", MapKeyField)
		}
		if f.Required && depth.IsCut(f.Type) {
			Fail(fp, "required recursive field (can't be unrolled to a fixed depth)")
		}
		x.walkType(fp, f.Type, depth)
	}
	x.checkDependentRequired(path, obj)
}

func (x *Index) walkType(path string, t ir.Type, depth Depth) {
	switch tt := t.(type) {
	case ir.String, ir.Bool, ir.Int, ir.Float:
	case ir.Object:
		x.walkObject(path, tt.Object, roleObject, "", depth)
	case ir.List:
		switch elem := tt.Elem.(type) {
		case ir.Object:
			x.walkObject(path, elem.Object, roleObject, "", depth)
		case ir.Union:
			x.walkUnion(path, elem.Union, depth)
		default:
			Fail(path, "list of %s", Describe(tt.Elem))
		}
	case ir.Map:
		switch elem := tt.Elem.(type) {
		case ir.String, ir.Bool, ir.Int, ir.Float:
		case ir.Object:
			x.walkObject(path, elem.Object, roleMapEntry, "", depth)
		default:
			Fail(path, "map of %s", Describe(tt.Elem))
		}
	default:
		Fail(path, "field type %s", Describe(t))
	}
}

func (x *Index) walkUnion(path string, u *ir.UnionDef, depth Depth) {
	if u.DiscriminatorField == "" {
		Fail(path, "union %s without a discriminator field", u.TypeName)
	}
	names := map[string]bool{}
	for _, v := range u.Variants {
		name := VariantName(v)
		if name == "" {
			Fail(path, "union %s variant %s without a discriminator value", u.TypeName, v.Object.TypeName)
		}
		if names[name] {
			Fail(path, "union %s with two variants named %q", u.TypeName, name)
		}
		names[name] = true
		x.walkObject(path+"."+name, v.Object, roleVariant, u.DiscriminatorField, depth)
	}
}

// Fields returns obj's fields as exposed in Terraform: the union discriminator and
// ExcludedFields are left out.
func (x *Index) Fields(obj *ir.ObjectDef) []ir.Field {
	d := x.discriminator[obj]
	out := make([]ir.Field, 0, len(obj.Fields))
	for _, f := range obj.Fields {
		if (d == "" || f.JSONName != d) && !isExcludedField(obj, f) {
			out = append(out, f)
		}
	}
	return out
}

// ExcludedFound returns the sorted ExcludedFields entries that matched a field of this facet.
func (x *Index) ExcludedFound() []string {
	out := make([]string, 0, len(x.excluded))
	for k := range x.excluded {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Discriminator returns the discriminator field of a union variant object.
func (x *Index) Discriminator(obj *ir.ObjectDef) (ir.Field, bool) {
	d := x.discriminator[obj]
	for _, f := range obj.Fields {
		if d != "" && f.JSONName == d {
			return f, true
		}
	}
	return ir.Field{}, false
}

// RequiredKeys returns the sorted snake_case names of obj's required Terraform fields.
func (x *Index) RequiredKeys(obj *ir.ObjectDef) []string {
	var keys []string
	for _, f := range x.Fields(obj) {
		if f.Required {
			keys = append(keys, f.Name)
		}
	}
	sort.Strings(keys)
	return keys
}

// checkDependentRequired fails unless every dependentRequired property of obj and each of its
// dependencies is a string attribute exposed in Terraform.
func (x *Index) checkDependentRequired(path string, obj *ir.ObjectDef) {
	byJSON := map[string]ir.Field{}
	for _, f := range x.Fields(obj) {
		byJSON[f.JSONName] = f
	}
	for prop, deps := range obj.DependentRequired {
		for _, n := range append([]string{prop}, deps...) {
			f, ok := byJSON[n]
			if !ok {
				Fail(path, "dependentRequired on %q, which is not a Terraform field", n)
			}
			if _, ok := f.Type.(ir.String); !ok {
				Fail(path+"."+f.Name, "dependentRequired on a %s field (only strings are supported)", Describe(f.Type))
			}
		}
	}
}

// DependentRequired returns obj's dependentRequired rules with snake_case names: for each
// field, the sibling fields that must be set when it is set. Dependencies are sorted.
func (x *Index) DependentRequired(obj *ir.ObjectDef) map[string][]string {
	if len(obj.DependentRequired) == 0 {
		return nil
	}
	snake := map[string]string{}
	for _, f := range x.Fields(obj) {
		snake[f.JSONName] = f.Name
	}
	out := make(map[string][]string, len(obj.DependentRequired))
	for prop, deps := range obj.DependentRequired {
		names := make([]string, 0, len(deps))
		for _, d := range deps {
			names = append(names, snake[d])
		}
		sort.Strings(names)
		out[snake[prop]] = names
	}
	return out
}

// IsMapEntry reports whether obj is the element of a Map{Object} (rendered as a list of
// entries with a MapKeyField attribute).
func (x *Index) IsMapEntry(obj *ir.ObjectDef) bool {
	return x.roles[obj] == roleMapEntry
}

// VariantName is the block name for a union variant: its discriminator value in lowercase.
func VariantName(v ir.Variant) string {
	return strings.ToLower(v.DiscriminatorValue)
}

// VariantGoName is the PascalCase Go field name for a union variant.
func VariantGoName(v ir.Variant) string {
	n := VariantName(v)
	if n == "" {
		return ""
	}
	return strings.ToUpper(n[:1]) + n[1:]
}

// IsScalar reports whether t maps to a Terraform scalar attribute.
func IsScalar(t ir.Type) bool {
	switch t.(type) {
	case ir.String, ir.Bool, ir.Int, ir.Float:
		return true
	}
	return false
}

// Describe names an IR type for error messages.
func Describe(t ir.Type) string {
	switch tt := t.(type) {
	case ir.List:
		return "list of " + Describe(tt.Elem)
	case ir.Map:
		return "map of " + Describe(tt.Elem)
	case ir.Object:
		return "object " + tt.Object.TypeName
	case ir.Union:
		return "union " + tt.Union.TypeName
	case ir.Enum:
		return "enum " + tt.Enum.TypeName
	case ir.DateTime:
		return "date-time"
	case ir.Any:
		return "untyped value"
	}
	return fmt.Sprintf("%T", t)
}

// FacetKey is the schema block name (and model tfsdk tag) for a facet: the facet name
// without the Facet/Job/Dataset suffixes, in snake_case (e.g. "job_type", "sql", "symlinks").
func FacetKey(facetName string) string {
	base := FacetBaseName(facetName)
	if base == "DatasetVersion" {
		return "version"
	}
	return toSnakeCase(base)
}

// FacetBaseName strips the Facet, Job and Dataset suffixes from a facet name.
func FacetBaseName(facetName string) string {
	base := strings.TrimSuffix(facetName, "Facet")
	base = strings.TrimSuffix(base, "Job")
	base = strings.TrimSuffix(base, "Dataset")
	return base
}

func toSnakeCase(s string) string {
	if s == "" {
		return ""
	}
	var b strings.Builder
	runes := []rune(s)
	for i, r := range runes {
		if unicode.IsUpper(r) {
			if i > 0 {
				prev := runes[i-1]
				nextLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
				if unicode.IsLower(prev) || nextLower {
					b.WriteByte('_')
				}
			}
			b.WriteRune(unicode.ToLower(r))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

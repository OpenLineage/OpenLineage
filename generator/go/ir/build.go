/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

// Package ir defines the Intermediate Representation used by the facet-gen code generator.
package ir

import (
	"fmt"
	"sort"

	"github.com/atombender/go-jsonschema/pkg/schemas"

	"github.com/OpenLineage/openlineage/generator/go/discover"
	"github.com/OpenLineage/openlineage/generator/go/genutil"
	"github.com/OpenLineage/openlineage/generator/go/resolve"
)

// buildCtx carries per-facet build state.
type buildCtx struct {
	objects      map[*schemas.Type]*ObjectDef // memoization / cycle detection
	unions       map[*schemas.Type]*UnionDef  // memoization for union (oneOf) schemas
	enums        map[*schemas.Type]*EnumDef   // memoization for enum string schemas
	inverseMap   map[*schemas.Type]string     // schema type pointer → $defs name
	filterFields bool                         // if true, drop fields from isExplicitlyExcludedField
}

// BuildFacet builds an IR Facet from a discovered schema facet.
// filterFields controls whether some known TF-incompatible fields are excluded.
func BuildFacet(f discover.Facet, r *resolve.Resolver, filterFields bool) Facet {
	ctx := &buildCtx{
		objects:      make(map[*schemas.Type]*ObjectDef),
		unions:       make(map[*schemas.Type]*UnionDef),
		enums:        make(map[*schemas.Type]*EnumDef),
		inverseMap:   r.InverseMap(),
		filterFields: filterFields,
	}

	root := r.Resolve(f.Schema)

	return Facet{
		Name:         f.Name,
		Kind:         f.Type,
		Root:         buildObject(ctx, root, r, f.Name),
		SchemaURL:    f.SchemaURL,
		ContainerKey: f.ContainerKey,
	}
}

// -----------------------------------------------------------------------------
// Object construction
// -----------------------------------------------------------------------------

func buildObject(
	ctx *buildCtx,
	t *schemas.Type,
	r *resolve.Resolver,
	suggestedName string,
) *ObjectDef {

	if t == nil {
		return &ObjectDef{
			TypeName: suggestedName,
			Required: map[string]bool{},
		}
	}

	// ✅ Cycle detection / memoization
	if existing, ok := ctx.objects[t]; ok {
		return existing
	}

	// Prefer the $defs canonical name over the suggested contextual name.
	typeName := suggestedName
	if name, ok := ctx.inverseMap[t]; ok {
		typeName = name
	}

	obj := &ObjectDef{
		TypeName:    typeName,
		Description: t.Description,
		Fields:      nil,
		Required:    map[string]bool{},
	}

	// ✅ Register early to break cycles
	ctx.objects[t] = obj

	// gatherSchemas splits contributing schemas:
	//   required → direct type:object and allOf items   (their Required arrays count)
	//   optional → oneOf variants, flattened recursively (fields merged in but NOT required)
	requiredSchemas, optionalSchemas := gatherSchemas(t, r)

	// 1) Required fields — only from required-side schemas
	for _, o := range requiredSchemas {
		for _, name := range o.Required {
			obj.Required[name] = true
		}
	}

	// 2) Properties — from all schemas (required + optional), first definition wins
	seen := map[string]bool{}
	for _, o := range append(requiredSchemas, optionalSchemas...) {
		for name, prop := range o.Properties {

			if ctx.filterFields && isExplicitlyExcludedField(name) {
				continue
			}

			if seen[name] {
				continue
			}
			seen[name] = true
			resolved := r.Resolve(prop)

			// Nested type name: currentTypeName + capitalised JSON key
			nestedName := typeName + export(name)

			field := Field{
				Name:        genutil.ToSnake(name),
				GoName:      export(name),
				JSONName:    name,
				Description: prop.Description,
				Type:        buildType(ctx, resolved, r, nestedName),
				Required:    obj.Required[name],
			}

			obj.Fields = append(obj.Fields, field)
		}
	}

	sort.Slice(obj.Fields, func(i, j int) bool {
		return obj.Fields[i].Name < obj.Fields[j].Name
	})

	return obj
}

// -----------------------------------------------------------------------------
// Type construction
// -----------------------------------------------------------------------------

func buildType(
	ctx *buildCtx,
	t *schemas.Type,
	r *resolve.Resolver,
	suggestedTypeName string,
) Type {

	if t == nil {
		return String{}
	}

	// oneOf always wins — must be checked before the type switch so that
	// schemas with both "type":"object" and "oneOf" (e.g. BaseSubsetCondition)
	// are correctly represented as discriminated unions, not merged flat structs.
	if len(t.OneOf) > 0 {
		return Union{Union: buildUnion(ctx, t, r, suggestedTypeName)}
	}

	switch primaryType(t) {

	case "string":
		if len(t.Enum) > 0 {
			return Enum{Enum: buildEnum(ctx, t, suggestedTypeName)}
		}
		if t.Format == "date-time" {
			return DateTime{}
		}
		return String{}

	case "boolean":
		return Bool{}

	case "integer":
		return Int{}

	case "number":
		return Float{}

	case "array":
		// Array element inherits the same suggested name; the inverseMap will
		// override it with the $defs name if the items type is a named ref.
		return List{
			Elem: buildType(ctx, r.Resolve(t.Items), r, suggestedTypeName),
		}

	case "object":
		// map-like object
		if t.AdditionalProperties != nil && len(t.Properties) == 0 {
			return Map{
				Elem: buildType(
					ctx,
					r.Resolve(t.AdditionalProperties),
					r,
					suggestedTypeName+"Value",
				),
			}
		}

		return Object{
			Object: buildObject(ctx, t, r, suggestedTypeName),
		}
	}

	return String{}
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

// gatherSchemas splits the schemas that contribute properties to an ObjectDef into two groups:
//   - required: direct type:object and allOf items — their Required arrays are honoured
//   - optional: oneOf variant schemas, flattened recursively — fields merged in but never required
//
// The split matters for discriminated unions: oneOf variants are mutually exclusive so none of
// their fields can be required on the merged supertype.
func gatherSchemas(t *schemas.Type, r *resolve.Resolver) (required, optional []*schemas.Type) {
	if primaryType(t) == "object" {
		required = append(required, t)
	}

	for _, sub := range t.AllOf {
		if primaryType(sub) == "object" {
			required = append(required, sub)
		}
	}

	for _, sub := range t.OneOf {
		resolved := r.Resolve(sub)
		if resolved == nil {
			continue
		}
		subReq, subOpt := gatherSchemas(resolved, r)
		// Everything from a oneOf branch is optional in the merged supertype
		optional = append(optional, subReq...)
		optional = append(optional, subOpt...)
	}

	return
}

func primaryType(t *schemas.Type) string {
	if t == nil || len(t.Type) == 0 {
		return ""
	}
	return t.Type[0]
}

// -----------------------------------------------------------------------------
// Union (oneOf) construction
// -----------------------------------------------------------------------------

// buildUnion constructs a UnionDef for a schema that has a oneOf clause.
// It memoizes by schema pointer so that the same interface is reused when the
// same union type is referenced from multiple fields.
func buildUnion(ctx *buildCtx, t *schemas.Type, r *resolve.Resolver, suggestedName string) *UnionDef {
	if existing, ok := ctx.unions[t]; ok {
		return existing
	}

	typeName := suggestedName
	if name, ok := ctx.inverseMap[t]; ok {
		typeName = name
	}

	u := &UnionDef{
		TypeName:    typeName,
		Description: t.Description,
	}
	// Register early so recursive refs (e.g. BinarySubsetCondition.left) don't loop.
	ctx.unions[t] = u

	// Resolve all variant schemas.
	var variantSchemas []*schemas.Type
	for _, sub := range t.OneOf {
		res := r.Resolve(sub)
		if res != nil {
			variantSchemas = append(variantSchemas, res)
		}
	}

	// Detect a common const-valued discriminator property across all variants.
	u.DiscriminatorField, _ = detectDiscriminatorField(variantSchemas, r)

	// Collect discriminator values in variant order.
	discValues := make([]string, len(variantSchemas))
	if u.DiscriminatorField != "" {
		for i, vs := range variantSchemas {
			if p, ok := flatProperties(vs, r)[u.DiscriminatorField]; ok {
				if res := r.Resolve(p); res != nil {
					if s, ok := res.Const.(string); ok {
						discValues[i] = s
					}
				}
			}
		}
	}

	for i, vs := range variantSchemas {
		var varName string
		if name, ok := ctx.inverseMap[vs]; ok {
			varName = name
		} else {
			varName = fmt.Sprintf("%sVariant%d", suggestedName, i)
		}
		u.Variants = append(u.Variants, Variant{
			DiscriminatorValue: discValues[i],
			Object:             buildObject(ctx, vs, r, varName),
		})
	}

	return u
}

// detectDiscriminatorField returns the property name whose value is a const
// string in every variant (i.e. a discriminator tag like "type":"binary").
// Searches each variant's direct properties and those visible through allOf.
func detectDiscriminatorField(variants []*schemas.Type, r *resolve.Resolver) (field string, values []string) {
	if len(variants) == 0 {
		return "", nil
	}

	for propName, propSchema := range flatProperties(variants[0], r) {
		res := r.Resolve(propSchema)
		if res == nil || res.Const == nil {
			continue
		}
		firstConst, ok := res.Const.(string)
		if !ok {
			continue
		}

		vals := []string{firstConst}
		allHave := true
		for _, v := range variants[1:] {
			vProp, ok := flatProperties(v, r)[propName]
			if !ok {
				allHave = false
				break
			}
			vRes := r.Resolve(vProp)
			if vRes == nil || vRes.Const == nil {
				allHave = false
				break
			}
			vConst, ok := vRes.Const.(string)
			if !ok {
				allHave = false
				break
			}
			vals = append(vals, vConst)
		}
		if allHave {
			return propName, vals
		}
	}
	return "", nil
}

// flatProperties collects all properties visible on a schema: its own Properties
// plus those contributed by allOf sub-schemas (first definition wins).
func flatProperties(t *schemas.Type, r *resolve.Resolver) map[string]*schemas.Type {
	props := make(map[string]*schemas.Type, len(t.Properties))
	for k, v := range t.Properties {
		props[k] = v
	}
	for _, sub := range t.AllOf {
		if resolved := r.Resolve(sub); resolved != nil {
			for k, v := range resolved.Properties {
				if _, exists := props[k]; !exists {
					props[k] = v
				}
			}
		}
	}
	return props
}

func export(s string) string {
	if s == "" {
		return s
	}
	return string(s[0]&^0x20) + s[1:]
}

// -----------------------------------------------------------------------------
// Enum construction
// -----------------------------------------------------------------------------

// buildEnum constructs an EnumDef for a string schema that has an enum clause.
func buildEnum(ctx *buildCtx, t *schemas.Type, suggestedName string) *EnumDef {
	if existing, ok := ctx.enums[t]; ok {
		return existing
	}

	typeName := suggestedName
	if name, ok := ctx.inverseMap[t]; ok {
		typeName = name
	}

	var values []string
	for _, v := range t.Enum {
		if s, ok := v.(string); ok {
			values = append(values, s)
		}
	}

	e := &EnumDef{TypeName: typeName, Values: values}
	ctx.enums[t] = e
	return e
}

func isExplicitlyExcludedField(fieldName string) bool {
	switch fieldName {
	case "transformationDescription",
		"transformationType":
		return true
	default:
		return false
	}
}

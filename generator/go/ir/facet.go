/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ir

import "github.com/OpenLineage/openlineage/generator/go/discover"

// Facet is the top-level IR type for a single OpenLineage facet.
type Facet struct {
	Name         string
	Kind         discover.FacetType
	Root         *ObjectDef
	SchemaURL    string // $id of the JSON schema file
	ContainerKey string // JSON key in the facets container struct (e.g. "columnLineage")
}

// ObjectDef represents an object type in the IR.
type ObjectDef struct {
	TypeName    string // Go struct name (from $defs or derived from context)
	Description string
	Fields      []Field
	Required    map[string]bool
}

// UnionDef represents a oneOf discriminated union.
// DiscriminatorField is the JSON property name used to dispatch between variants ("type" most commonly).
// It is empty when no common const-valued property is found across all variants.
type UnionDef struct {
	TypeName           string // Go interface name (e.g. "BaseSubsetCondition")
	Description        string
	DiscriminatorField string // e.g. "type"; empty if not detectable
	Variants           []Variant
}

// Variant is one concrete option within a UnionDef.
type Variant struct {
	DiscriminatorValue string     // const value of the discriminator field (e.g. "binary"); empty if unknown
	Object             *ObjectDef // concrete struct for this variant
}

// EnumDef represents a string type constrained to a fixed set of named values.
type EnumDef struct {
	TypeName string   // Go type name (e.g. "LifecycleStateChangeDatasetFacetLifecycleStateChange")
	Values   []string // raw enum string values (e.g. ["ALTER", "CREATE", …])
}

// Field is a single property of an ObjectDef.
type Field struct {
	Name        string // snake_case (used as tfsdk tag)
	GoName      string // PascalCase Go field name
	JSONName    string // original camelCase JSON property key (for json:"…" tags)
	Description string
	Type        Type
	Required    bool
}

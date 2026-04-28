/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

// Package ir defines the Intermediate Representation used by the facet-gen code generator.
package ir

// Type is the IR type discriminant; all concrete IR types implement this interface.
type Type interface {
	isIRType()
}

// String represents a JSON Schema string type.
type String struct{}

// Bool represents a JSON Schema boolean type.
type Bool struct{}

// Int represents a JSON Schema integer type.
type Int struct{}

// Float represents a JSON Schema number type.
type Float struct{}

// DateTime represents a JSON Schema string with format "date-time".
// Maps to *time.Time in OL client structs.
type DateTime struct{}

// List represents a JSON Schema array type.
type List struct {
	Elem Type
}

// Map represents a JSON Schema object with additionalProperties (a string-keyed map).
type Map struct {
	Elem Type
}

// Object represents a JSON Schema object type resolved to a named struct.
type Object struct {
	Object *ObjectDef
}

// Union represents a oneOf discriminated union (interface + concrete variants).
type Union struct {
	Union *UnionDef
}

// Enum represents a string type constrained to a fixed set of values.
type Enum struct {
	Enum *EnumDef
}

func (String) isIRType()   {}
func (Bool) isIRType()     {}
func (Int) isIRType()      {}
func (Float) isIRType()    {}
func (DateTime) isIRType() {}
func (List) isIRType()     {}
func (Map) isIRType()      {}
func (Object) isIRType()   {}
func (Union) isIRType()    {}
func (Enum) isIRType()     {}

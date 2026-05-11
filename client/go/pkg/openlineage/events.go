/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package openlineage

import (
	"github.com/OpenLineage/openlineage/client/go/pkg/facets"
)

// OpenLineage spec schema URLs for different event types
const (
	RunEventSchemaURL     = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
	DatasetEventSchemaURL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/DatasetEvent"
	JobEventSchemaURL     = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent"
)

// DefaultNamespace is the default namespace used when none is specified.
var DefaultNamespace = "default"

// NewInputElement creates a new InputElement with the given name and namespace.
func NewInputElement(name, namespace string) InputElement {
	return InputElement{
		Name:      name,
		Namespace: namespace,
	}
}

// WithInputFacets adds input dataset facets to this InputElement.
func (ie InputElement) WithInputFacets(fs ...facets.InputDatasetFacet) InputElement {
	for _, f := range fs {
		f.Apply(&ie.InputFacets)
	}

	return ie
}

// WithFacets adds dataset facets to this InputElement.
func (ie InputElement) WithFacets(fs ...facets.DatasetFacet) InputElement {
	for _, f := range fs {
		f.Apply(&ie.Facets)
	}

	return ie
}

// NewOutputElement creates a new OutputElement with the given name and namespace.
func NewOutputElement(name, namespace string) OutputElement {
	return OutputElement{
		Name:      name,
		Namespace: namespace,
	}
}

// WithOutputFacets adds output dataset facets to this OutputElement.
func (oe OutputElement) WithOutputFacets(fs ...facets.OutputDatasetFacet) OutputElement {
	for _, f := range fs {
		f.Apply(&oe.OutputFacets)
	}

	return oe
}

// WithFacets adds dataset facets to this OutputElement.
func (oe OutputElement) WithFacets(fs ...facets.DatasetFacet) OutputElement {
	for _, f := range fs {
		f.Apply(&oe.Facets)
	}

	return oe
}

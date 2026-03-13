/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package openlineage

import (
	"github.com/google/uuid"
)

// NewRunID generates a new UUID v7 for use as a run ID.
// UUID v7 is time-ordered and recommended for run IDs as it provides
// better database performance and natural chronological sorting.
func NewRunID() uuid.UUID {
	return uuid.Must(uuid.NewV7())
}

// Ptr returns a pointer to the given value.
// Useful for setting optional fields in facets.
//
// Example:
//
//	facets.NewSchema(producer).WithFields([]facets.FieldElement{
//	    {Name: "id", Type: Ptr("BIGINT")},
//	    {Name: "name", Type: Ptr("VARCHAR")},
//	})
func Ptr[T any](v T) *T {
	return &v
}

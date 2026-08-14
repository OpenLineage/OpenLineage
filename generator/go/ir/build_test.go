/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ir

import (
	"reflect"
	"testing"

	"github.com/atombender/go-jsonschema/pkg/schemas"

	"github.com/OpenLineage/openlineage/generator/go/resolve"
)

func TestDetectDiscriminatorFieldSupportsSingletonEnums(t *testing.T) {
	variants := []*schemas.Type{
		{
			Properties: map[string]*schemas.Type{
				"type": {Enum: []any{"DATASET"}},
			},
		},
		{
			Properties: map[string]*schemas.Type{
				"type": {Enum: []any{"JOB"}},
			},
		},
	}

	field, values := detectDiscriminatorField(variants, resolve.New(nil))
	if field != "type" {
		t.Fatalf("detectDiscriminatorField() field = %q, want %q", field, "type")
	}
	want := []string{"DATASET", "JOB"}
	if !reflect.DeepEqual(values, want) {
		t.Fatalf("detectDiscriminatorField() values = %v, want %v", values, want)
	}
}

// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

package validator

import (
	"testing"
)

func TestValidate(t *testing.T) {
	validator := New()
	err := validator.Validate("{}")
	if err == nil {
		t.Fail()
	}
}

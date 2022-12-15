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

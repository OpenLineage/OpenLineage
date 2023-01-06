// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

package validator

type Validator struct{}

type IEventValidator interface {
	Validate(event string) error
}

func (Validator *Validator) Validate(event string) (err error) {
	// Not yet implemented.
	return nil
}

func New() *Validator {
	return &Validator{}
}

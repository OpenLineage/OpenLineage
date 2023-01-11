// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

package transports

type TransportError struct {
	StatusCode int
	message    string
}

func (e *TransportError) Error() string {
	return e.message
}

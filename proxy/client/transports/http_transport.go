// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

package transports

type HttpTransport struct {
	OpenLineageURL    string
	OpenLineageAPIKey string
}

func (transport *HttpTransport) Emit(lineageEvent string) error {
	return nil
}

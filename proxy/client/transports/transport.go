// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

package transports

import (
	"errors"
	"github.com/OpenLineage/OpenLineage/client-proxy/config"
)

type ITransport interface {
	Emit(lineageEvent string) error
}

func Create(conf config.Config) (ITransport, error) {
	switch conf.TransportType {
	case "http":
		return &HttpTransport{OpenLineageURL: conf.TransportURL, OpenLineageAPIKey: conf.AuthKey}, nil
	default:
		return nil, errors.New("unknown transport type")
	}
}

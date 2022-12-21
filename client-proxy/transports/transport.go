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

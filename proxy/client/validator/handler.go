package validator

import (
	"errors"
	"github.com/OpenLineage/OpenLineage/client-proxy/config"
)

type IFailedEventHandler interface {
	Handle(failedEvent string)
}

func NewFailedEventHandler(conf config.Config) (IFailedEventHandler, error) {
	switch conf.FailedEventHandlerType {
	case "logging":
		return &LoggingHandler{}, nil
	default:
		return nil, errors.New("unknown failed event handler type")
	}

}

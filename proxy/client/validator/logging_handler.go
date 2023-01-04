package validator

import "log"

type LoggingHandler struct{}

func (handler *LoggingHandler) Handle(failedEvent string) {
	log.Println(failedEvent)
}

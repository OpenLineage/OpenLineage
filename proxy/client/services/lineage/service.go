package lineage

import (
	"github.com/OpenLineage/OpenLineage/client-proxy/database"
	"github.com/OpenLineage/OpenLineage/client-proxy/logger"
	"github.com/OpenLineage/OpenLineage/client-proxy/validator"
	"net/http"
)

type Service struct {
	validator          validator.IEventValidator
	failedEventHandler validator.IFailedEventHandler
	eventLogger        logger.IEventLogger
}

type ILineageService interface {
	CreateLineage(lineageEvent string) int
}

func (s *Service) CreateLineage(lineageEvent string) int {
	err := s.validator.Validate(lineageEvent)
	if err != nil {
		go s.failedEventHandler.Handle(lineageEvent)
		return http.StatusBadRequest
	}

	s.eventLogger.Log(lineageEvent)
	return http.StatusCreated
}

func New(db database.IDatabase, h validator.IFailedEventHandler) (*Service, error) {
	v, err := validator.New()
	if err != nil {
		return nil, err
	}

	return &Service{
		validator:          v,
		failedEventHandler: h,
		eventLogger:        logger.New(db),
	}, nil
}

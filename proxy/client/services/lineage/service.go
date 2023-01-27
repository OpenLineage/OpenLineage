// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"github.com/OpenLineage/OpenLineage/client-proxy/config"
	"github.com/OpenLineage/OpenLineage/client-proxy/logger"
	"github.com/OpenLineage/OpenLineage/client-proxy/storage"
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
	if err := s.validator.Validate(lineageEvent); err != nil {
		go s.failedEventHandler.Handle(lineageEvent)
		return http.StatusBadRequest
	}

	if err := s.eventLogger.Log(lineageEvent); err != nil {
		go s.failedEventHandler.Handle(lineageEvent)
		return http.StatusInternalServerError
	}

	return http.StatusCreated
}

func New(conf config.Config, storage storage.IStorage, h validator.IFailedEventHandler) (*Service, error) {
	v, err := validator.New()
	if err != nil {
		return nil, err
	}

	return &Service{
		validator:          v,
		failedEventHandler: h,
		eventLogger:        logger.New(conf, storage),
	}, nil
}

// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

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

func (service *Service) CreateLineage(lineageEvent string) int {
	err := service.validator.Validate(lineageEvent)
	if err != nil {
		go service.failedEventHandler.Handle(lineageEvent)
		return http.StatusBadRequest
	}

	service.eventLogger.Log(lineageEvent)
	return http.StatusCreated
}

func New(db database.IDatabase, failedEventHandler validator.IFailedEventHandler) *Service {
	return &Service{
		validator:          validator.New(),
		failedEventHandler: failedEventHandler,
		eventLogger:        logger.New(db),
	}
}

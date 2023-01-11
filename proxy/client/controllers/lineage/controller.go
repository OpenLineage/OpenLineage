// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"github.com/OpenLineage/OpenLineage/client-proxy/services/lineage"
	"github.com/gin-gonic/gin"
	"io"
)

type Controller struct {
	service lineage.ILineageService
}

type ILineageController interface {
	PostLineage(ctx *gin.Context)
}

func (controller *Controller) PostLineage(ctx *gin.Context) {
	body, _ := io.ReadAll(ctx.Request.Body)
	lineageEvent := string(body)
	status := controller.service.CreateLineage(lineageEvent)
	ctx.Status(status)
}

func New(service lineage.ILineageService) *Controller {
	return &Controller{service: service}
}

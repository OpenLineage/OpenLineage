// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"github.com/OpenLineage/OpenLineage/client-proxy/config"
	"github.com/OpenLineage/OpenLineage/client-proxy/consumer"
	LineageController "github.com/OpenLineage/OpenLineage/client-proxy/controllers/lineage"
	LineageService "github.com/OpenLineage/OpenLineage/client-proxy/services/lineage"
	"github.com/OpenLineage/OpenLineage/client-proxy/storage"
	"github.com/OpenLineage/OpenLineage/client-proxy/transports"
	"github.com/OpenLineage/OpenLineage/client-proxy/validator"
	"github.com/gin-gonic/gin"
	"log"
)

func main() {
	conf, err := config.Load(".")
	if err != nil {
		log.Fatalln("Cannot load config:", err)
	}

	err = storage.Migrate(conf)
	if err != nil {
		log.Fatalln(err)
	}

	storage, err := storage.New(conf)
	if err != nil {
		log.Fatalln(err)
	}

	failedEventHandler, err := validator.NewFailedEventHandler(conf)
	if err != nil {
		log.Fatalln(err)
	}
	lineageService, err := LineageService.New(conf, storage, failedEventHandler)
	if err != nil {
		log.Fatalln(err)
	}
	lineageController := LineageController.New(lineageService)

	transport, err := transports.Create(conf)
	if err != nil {
		log.Fatalln(err)
	}
	lineageEventConsumer := consumer.New(storage, transport)
	go lineageEventConsumer.Run()

	router := gin.Default()
	router.Use(gin.Logger())
	router.Use(gin.Recovery())
	router.POST("/api/v1/lineage", lineageController.PostLineage)

	log.Fatalln(router.Run(":8080"))
}

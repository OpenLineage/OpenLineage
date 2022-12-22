package main

import (
	"github.com/OpenLineage/OpenLineage/client-proxy/config"
	"github.com/OpenLineage/OpenLineage/client-proxy/consumer"
	LineageController "github.com/OpenLineage/OpenLineage/client-proxy/controllers/lineage"
	"github.com/OpenLineage/OpenLineage/client-proxy/database"
	LineageService "github.com/OpenLineage/OpenLineage/client-proxy/services/lineage"
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

	db := database.New(conf)

	failedEventHandler, err := validator.NewFailedEventHandler(conf)
	if err != nil {
		log.Fatalln(err)
	}
	lineageService := LineageService.New(db, failedEventHandler)
	lineageController := LineageController.New(lineageService)

	transport, err := transports.Create(conf)
	if err != nil {
		log.Fatalln(err)
	}
	lineageEventConsumer := consumer.New(db, transport)
	go lineageEventConsumer.Run()

	router := gin.Default()
	router.Use(gin.Logger())
	router.Use(gin.Recovery())
	router.POST("/api/v1/lineage", lineageController.PostLineage)

	log.Fatalln(router.Run(":8080"))
}

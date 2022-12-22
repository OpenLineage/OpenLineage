package consumer

import (
	"github.com/OpenLineage/OpenLineage/client-proxy/database"
	"github.com/OpenLineage/OpenLineage/client-proxy/transports"
)

type Consumer struct {
	db        database.IDatabase
	transport transports.ITransport
}

type IConsumer interface {
	Run()
}

func (consumer *Consumer) Run() {
	// Not yet implemented
	// Consume lineage events from database in an infinite loop and send it to transport
}

func New(db database.IDatabase, transport transports.ITransport) *Consumer {
	return &Consumer{db: db, transport: transport}
}

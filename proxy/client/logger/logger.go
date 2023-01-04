package logger

import (
	"github.com/OpenLineage/OpenLineage/client-proxy/database"
	"log"
)

type EventLogger struct {
	channel chan string
	db      database.IDatabase
}

type IEventLogger interface {
	Log(lineageEvent string)
}

func (logger *EventLogger) Log(lineageEvent string) {
	logger.channel <- lineageEvent
}

func New(db database.IDatabase) *EventLogger {
	ch := make(chan string)
	go Log(ch)
	return &EventLogger{channel: ch, db: db}
}

func Log(ch chan string) {
	for lineageEvent := range ch {
		// Not yet implemented.
		// Log lineage event to the local database.
		log.Println(lineageEvent)
	}
}

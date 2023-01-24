// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

package logger

import (
	"github.com/OpenLineage/OpenLineage/client-proxy/config"
	"github.com/OpenLineage/OpenLineage/client-proxy/database"
	"sync"
	"time"
)

type EventLogger struct {
	db                  database.IDatabase
	partitionLimitHours uint
	partitionLimitBytes uint64
	lock                sync.Mutex
}

// IEventLogger is an interface for lineage event logging
type IEventLogger interface {
	Log(lineageEvent string) error
}

func (l *EventLogger) Log(lineageEvent string) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	p, err := l.db.GetCurrentPartition()
	if err != nil {
		return err
	}

	continuePartition := p.Size+uint64(len(lineageEvent)) < l.partitionLimitBytes &&
		p.CreatedAt.Add(time.Hour*time.Duration(l.partitionLimitHours)).After(time.Now())

	if !continuePartition {
		p, err = l.db.RotatePartition()
		if err != nil {
			return err
		}
	}

	return l.db.InsertLineageEvent(p, lineageEvent)
}

func New(conf config.Config, db database.IDatabase) *EventLogger {
	return &EventLogger{
		db:                  db,
		partitionLimitHours: conf.PartitionLimitHours,
		partitionLimitBytes: conf.PartitionLimitBytes,
	}
}

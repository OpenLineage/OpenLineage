// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

package logger

import (
	"github.com/OpenLineage/OpenLineage/client-proxy/config"
	"github.com/OpenLineage/OpenLineage/client-proxy/storage"
	"sync"
	"time"
)

type EventLogger struct {
	storage             storage.IStorage
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

	p, err := l.storage.GetCurrentPartition()
	if err != nil {
		return err
	}

	continuePartition := p.Size+uint64(len(lineageEvent)) < l.partitionLimitBytes &&
		p.CreatedAt.Add(time.Hour*time.Duration(l.partitionLimitHours)).After(time.Now())

	if !continuePartition {
		p, err = l.storage.RotatePartition()
		if err != nil {
			return err
		}
	}

	return l.storage.InsertLineageEvent(p, lineageEvent)
}

func New(conf config.Config, storage storage.IStorage) *EventLogger {
	return &EventLogger{
		storage:             storage,
		partitionLimitHours: conf.PartitionLimitHours,
		partitionLimitBytes: conf.PartitionLimitBytes,
	}
}

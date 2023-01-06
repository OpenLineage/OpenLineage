// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

package validator

import "log"

type LoggingHandler struct{}

func (handler *LoggingHandler) Handle(failedEvent string) {
	log.Println(failedEvent)
}

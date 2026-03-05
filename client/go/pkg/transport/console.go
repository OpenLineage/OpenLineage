/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package transport //nolint:revive // package comment is in transport.go

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tidwall/pretty"
)

// ConsoleConfig holds configuration for the console transport.
type ConsoleConfig struct {
	PrettyPrint bool
}

type consoleTransport struct {
	prettyPrint bool
}

func (ct *consoleTransport) Emit(_ context.Context, event any) error {
	body, err := json.Marshal(&event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	if ct.prettyPrint {
		body = pretty.Pretty(body)
	}

	if _, err := fmt.Println(string(body)); err != nil {
		return fmt.Errorf("emit event to console: %w", err)
	}

	return nil
}

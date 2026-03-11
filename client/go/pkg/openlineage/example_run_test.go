/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package openlineage_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	ol "github.com/OpenLineage/openlineage/client/go/pkg/openlineage"
	"github.com/OpenLineage/openlineage/client/go/pkg/transport"
)

func ExampleRun() {
	ctx := context.Background()

	producer := "https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/go"
	cfg := ol.ClientConfig{
		Transport: transport.Config{
			Type: transport.TransportTypeConsole,
			Console: transport.ConsoleConfig{
				PrettyPrint: true,
			},
		},
	}
	client, err := ol.NewClient(producer, &cfg)
	if err != nil {
		slog.Error("ol.NewClient failed", "error", err)
	}

	ctx, run, err := client.StartRun(ctx, "ingest")
	if err != nil {
		slog.Error("client.StartRun failed", "error", err)
		return
	}
	defer run.Finish()

	if err := ChildFunction(ctx); err != nil {

		slog.Warn("child function failed", "error", err)
	}

}

func ChildFunction(ctx context.Context) error {
	parent := ol.RunFromContext(ctx)
	_, childRun, err := parent.StartChild(ctx, "child")
	if err != nil {
		return fmt.Errorf("start child run: %w", err)
	}

	err = DoWork()
	childRun.Finish(err)

	return err
}

func DoWork() error {
	return errors.New("did not do work")
}

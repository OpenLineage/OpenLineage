package openlineage_test

import (
	"context"
	"errors"
	"log/slog"

	ol "github.com/ThijsKoot/openlineage/client/go/pkg/openlineage"
	"github.com/ThijsKoot/openlineage/client/go/pkg/transport"
)

func ExampleRun() {
	ctx := context.Background()

	cfg := ol.ClientConfig{
		Transport: transport.Config{
			Type: transport.TransportTypeConsole,
			Console: transport.ConsoleConfig{
				PrettyPrint: true,
			},
		},
	}
	client, err := ol.NewClient(cfg)
	if err != nil {
		slog.Error("ol.NewClient failed", "error", err)
	}

	ctx, run := client.StartRun(ctx, "ingest")
	defer run.Finish()

	if err := ChildFunction(ctx); err != nil {
		run.RecordError(err)

		slog.Warn("child function failed", "error", err)
	}

}

func ChildFunction(ctx context.Context) error {
	parent := ol.RunFromContext(ctx)
	_, childRun := parent.StartChild(ctx, "child")
	defer childRun.Finish()

	if err := DoWork(); err != nil {
		// Record the error in this run.
		// Finish() will emit a FAIL event.
		childRun.RecordError(err)

		return err
	}

	return nil
}

func DoWork() error {
	return errors.New("did not do work")
}

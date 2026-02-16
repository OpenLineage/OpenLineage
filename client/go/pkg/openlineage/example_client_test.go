package openlineage_test

import (
	"context"
	"log/slog"

	ol "github.com/OpenLineage/openlineage/client/go/pkg/openlineage"
	"github.com/OpenLineage/openlineage/client/go/pkg/transport"
	"github.com/google/uuid"
)

func ExampleClient() {
	producer := "https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/go"
	cfg := ol.ClientConfig{
		Transport: transport.Config{
			Type: transport.TransportTypeConsole,
			Console: transport.ConsoleConfig{
				PrettyPrint: true,
			},
		},
	}

	client, err := ol.NewClient(producer, cfg)
	if err != nil {
		slog.Error("ol.NewClient failed", "error", err)
	}

	ctx := context.Background()
	runID := uuid.Must(uuid.NewV7())
	event := ol.NewRunEvent(ol.EventTypeStart, runID, "foo-job", producer)

	if err := client.Emit(ctx, event); err != nil {
		slog.Error("emitting event failed", "error", err)
	}
}

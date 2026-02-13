# OpenLineage SDK for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/ThijsKoot/openlineage/client/go.svg)](https://pkg.go.dev/github.com/ThijsKoot/openlineage/client/go)

Go SDK for [OpenLineage](https://openlineage.io), a standard for data lineage collection and analysis.

## Features

- Full support for OpenLineage spec v2
- Multiple transport backends (HTTP, Console, GCP Lineage)
- Ergonomic Run API with automatic parent-child relationships
- Context-based run propagation
- Generated facet types with type-safe builders
- Fluent API for constructing events

## Installation

```bash
go get github.com/ThijsKoot/openlineage/client/go
```

## Quick Start

### Using the Default Client

The simplest way to emit events is using the default client with console output:

```go
package main

import (
    "github.com/google/uuid"
    ol "github.com/ThijsKoot/openlineage/client/go/pkg/openlineage"
)

func main() {
    // Create and emit a simple run event
    runID := uuid.New()
    event := ol.NewRunEvent(ol.EventTypeStart, runID, "my-job")
    event.Emit()
}
```

### Creating a Custom Client

```go
package main

import (
    "context"
    "log"

    ol "github.com/ThijsKoot/openlineage/client/go/pkg/openlineage"
    "github.com/ThijsKoot/openlineage/client/go/pkg/transport"
)

func main() {
    cfg := ol.ClientConfig{
        Namespace: "my-namespace",
        Transport: transport.Config{
            Type: transport.TransportTypeHTTP,
            HTTP: transport.HTTPConfig{
                URL:    "http://localhost:5000",
                APIKey: "your-api-key",
            },
        },
    }

    client, err := ol.NewClient(cfg)
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()
    
    // Start a run - this emits a START event
    ctx, run := client.StartRun(ctx, "etl-pipeline")
    defer run.Finish() // Emits COMPLETE or FAIL based on whether errors occurred
    
    // Do work...
}
```

## Core Concepts

### Events

OpenLineage defines three types of events:

- **RunEvent**: Represents a job execution (run) with START, RUNNING, COMPLETE, ABORT, FAIL, or OTHER states
- **JobEvent**: Static job metadata without run information
- **DatasetEvent**: Static dataset metadata

### Runs

The `Run` interface provides an ergonomic way to instrument your code:

```go
ctx, run := client.StartRun(ctx, "my-job")
defer run.Finish()

// Record inputs and outputs
run.RecordInputs(
    ol.NewInputElement("users", "postgres://db/schema"),
    ol.NewInputElement("orders", "postgres://db/schema"),
)

run.RecordOutputs(
    ol.NewOutputElement("user_orders", "s3://bucket/warehouse"),
)

// If something goes wrong
if err := doWork(); err != nil {
    run.RecordError(err) // Marks run as failed, Finish() will emit FAIL
    return err
}
```

### Parent-Child Relationships

Runs automatically track parent-child relationships through context:

```go
func ProcessData(ctx context.Context) {
    // Get parent run from context
    parent := ol.RunFromContext(ctx)
    
    // Create a child run - automatically linked to parent
    ctx, childRun := parent.StartChild(ctx, "process-step")
    defer childRun.Finish()
    
    // Child run's context can be passed further
    TransformData(ctx)
}
```

### Facets

Facets add metadata to runs, jobs, and datasets. The SDK provides type-safe generated facets:

```go
import "github.com/ThijsKoot/openlineage/client/go/pkg/facets"

// Create an event with facets
event := ol.NewRunEvent(ol.EventTypeStart, runID, "my-job").
    WithRunFacets(
        &facets.NominalTime{
            NominalStartTime: "2024-01-15T10:00:00Z",
            NominalEndTime:   "2024-01-15T11:00:00Z",
        },
        &facets.ProcessingEngine{
            Version: "3.5.0",
            Name:    ptr("Apache Spark"),
        },
    ).
    WithJobFacets(
        &facets.SQL{
            Query: "SELECT * FROM users",
        },
        &facets.JobType{
            ProcessingType: "BATCH",
            Integration:    "SPARK",
            JobType:        ptr("QUERY"),
        },
    )
```

### Datasets

Add input and output datasets to your events:

```go
input := ol.NewInputElement("source_table", "postgres://host/db").
    WithFacets(
        &facets.Schema{
            Fields: []facets.SchemaDatasetFacetFields{
                {Name: "id", Type: ptr("BIGINT")},
                {Name: "name", Type: ptr("VARCHAR")},
            },
        },
        &facets.DataSource{
            Name: ptr("production-postgres"),
            URI:  ptr("postgres://prod-db.example.com:5432/analytics"),
        },
    ).
    WithInputFacets(
        &facets.DataQualityMetrics{
            RowCount: ptr(int64(1000000)),
            Bytes:    ptr(int64(524288000)),
            ColumnMetrics: map[string]facets.ColumnMetrics{
                "id": {
                    NullCount:     ptr(int64(0)),
                    DistinctCount: ptr(int64(1000000)),
                },
            },
        },
    )

output := ol.NewOutputElement("target_table", "s3://bucket/warehouse").
    WithOutputFacets(
        &facets.OutputStatistics{
            RowCount:  ptr(int64(500000)),
            Size:      ptr(int64(262144000)),
            FileCount: ptr(int64(10)),
        },
    )

event := ol.NewRunEvent(ol.EventTypeComplete, runID, "etl-job").
    WithInputs(input).
    WithOutputs(output)
```

## Transports

### Console Transport

Outputs events to stdout. Useful for debugging and development.

```go
transport.Config{
    Type: transport.TransportTypeConsole,
    Console: transport.ConsoleConfig{
        PrettyPrint: true,
    },
}
```

### HTTP Transport

Sends events to an HTTP endpoint (e.g., [Marquez](https://marquezproject.ai/)).

```go
transport.Config{
    Type: transport.TransportTypeHTTP,
    HTTP: transport.HTTPConfig{
        URL:      "http://localhost:5000",
        Endpoint: "api/v1/lineage", // optional, default
        APIKey:   "your-api-key",   // optional
    },
}
```

### GCP Lineage Transport

Sends events to [Google Cloud Data Catalog Lineage API](https://cloud.google.com/data-catalog/docs/concepts/about-data-lineage).

```go
import (
    "context"
    "github.com/ThijsKoot/openlineage/client/go/pkg/transport"
)

// Using the dedicated constructor with options
ctx := context.Background()
t, err := transport.NewGCPLineageTransport(ctx, "my-gcp-project",
    transport.WithLocation("us-central1"),             // optional, default: us-central1
    transport.WithCredentialsFile("/path/to/sa.json"), // optional, uses ADC if not provided
)

// Or using the Config struct
cfg := ol.ClientConfig{
    Transport: transport.Config{
        Type: transport.TransportTypeGCPLineage,
        GCPLineage: transport.GCPLineageConfig{
            ProjectID:       "my-gcp-project",
            Location:        "us-central1",
            CredentialsFile: "/path/to/service-account.json", // optional
        },
    },
}
client, err := ol.NewClient(cfg)
```

#### Authentication

The GCP Lineage transport supports:

1. **Application Default Credentials (ADC)**: Used when `CredentialsFile` is not specified
2. **Service Account Key File**: Specify path via `CredentialsFile`

#### Required IAM Permissions

- `datalineage.events.create` on the project/location
- Or the predefined role: `roles/datalineage.producer`

## Disabling the Client

For testing or conditional instrumentation:

```go
cfg := ol.ClientConfig{
    Disabled: true, // All Emit calls become no-ops
}
client, _ := ol.NewClient(cfg)
```

## Complete Example

```go
package main

import (
    "context"
    "log"

    "github.com/google/uuid"
    ol "github.com/ThijsKoot/openlineage/client/go/pkg/openlineage"
    "github.com/ThijsKoot/openlineage/client/go/pkg/facets"
    "github.com/ThijsKoot/openlineage/client/go/pkg/transport"
)

func ptr[T any](v T) *T { return &v }

func main() {
    // Create client
    client, err := ol.NewClient(ol.ClientConfig{
        Namespace: "analytics",
        Transport: transport.Config{
            Type: transport.TransportTypeHTTP,
            HTTP: transport.HTTPConfig{
                URL: "http://marquez:5000",
            },
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()
    
    // Start the main pipeline run
    ctx, run := client.StartRun(ctx, "daily-etl")
    defer run.Finish()

    // Record job metadata
    run.RecordJobFacets(
        &facets.JobType{
            ProcessingType: "BATCH",
            Integration:    "SPARK",
        },
        &facets.SQL{
            Query: "INSERT INTO analytics.summary SELECT * FROM staging.events",
        },
    )

    // Record inputs
    run.RecordInputs(
        ol.NewInputElement("events", "postgres://staging/public").
            WithFacets(
                &facets.Schema{
                    Fields: []facets.SchemaDatasetFacetFields{
                        {Name: "event_id", Type: ptr("UUID")},
                        {Name: "event_type", Type: ptr("VARCHAR")},
                        {Name: "timestamp", Type: ptr("TIMESTAMP")},
                    },
                },
            ),
    )

    // Do the actual work
    if err := processData(ctx); err != nil {
        run.RecordError(err)
        log.Printf("Pipeline failed: %v", err)
        return
    }

    // Record outputs
    run.RecordOutputs(
        ol.NewOutputElement("summary", "s3://analytics/warehouse").
            WithOutputFacets(
                &facets.OutputStatistics{
                    RowCount: ptr(int64(1000000)),
                    Size:     ptr(int64(104857600)),
                },
            ),
    )
}

func processData(ctx context.Context) error {
    // Get the current run from context
    parent := ol.RunFromContext(ctx)
    
    // Create a child run for a sub-task
    ctx, childRun := parent.StartChild(ctx, "transform-step")
    defer childRun.Finish()
    
    // Processing logic here...
    return nil
}
```

## API Reference

### Client

| Method | Description |
|--------|-------------|
| `NewClient(cfg)` | Create a new client with configuration |
| `Emit(ctx, event)` | Emit an event using the configured transport |
| `NewRun(ctx, job)` | Create a new run (without emitting) |
| `StartRun(ctx, job)` | Create and start a run (emits START) |
| `ExistingRun(ctx, job, runID)` | Recreate a run for an existing run ID |

### Run

| Method | Description |
|--------|-------------|
| `RunID()` | Get the run's UUID |
| `JobName()` | Get the job name |
| `JobNamespace()` | Get the job namespace |
| `Parent()` | Get the parent run (if any) |
| `NewChild(ctx, job)` | Create a child run |
| `StartChild(ctx, job)` | Create and start a child run |
| `NewEvent(eventType)` | Create an event for this run |
| `Finish()` | Complete the run (COMPLETE or FAIL) |
| `RecordError(err)` | Record an error (marks run as failed) |
| `RecordRunFacets(...)` | Emit run facets |
| `RecordJobFacets(...)` | Emit job facets |
| `RecordInputs(...)` | Emit input datasets |
| `RecordOutputs(...)` | Emit output datasets |

### Events

| Function | Description |
|----------|-------------|
| `NewRunEvent(type, runID, job)` | Create a run event |
| `NewJobEvent(name)` | Create a job event |
| `NewDatasetEvent(name, namespace)` | Create a dataset event |

## Contributing

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines.

## License

Apache License 2.0 - see [LICENSE](../../LICENSE) for details.


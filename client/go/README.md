# OpenLineage SDK for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/OpenLineage/openlineage/client/go.svg)](https://pkg.go.dev/github.com/OpenLineage/openlineage/client/go)

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
go get github.com/OpenLineage/openlineage/client/go
```

## Quick Start

### Quick Start

```go
package main

import (
    "context"
    "log"

    ol "github.com/OpenLineage/openlineage/client/go/pkg/openlineage"
    "github.com/OpenLineage/openlineage/client/go/pkg/transport"
)

func main() {
    producer := "https://github.com/your-org/your-integration/tree/v1.0.0"

    client, err := ol.NewClient(producer, &ol.ClientConfig{
        Namespace: "my-namespace",
        Transport: transport.Config{
            Type: transport.TransportTypeHTTP,
            HTTP: transport.HTTPConfig{
                URL:    "http://localhost:5000",
                APIKey: "your-api-key",
            },
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()

    // Start a run — emits a START event
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

OpenLineage provides two approaches for emitting events:

#### 1. Using the Run Interface (Recommended for most cases)

The `Run` interface provides an ergonomic way to instrument your code with automatic lifecycle management:

```go
ctx, run := client.StartRun(ctx, "my-job")
defer run.Finish()

// Build and emit an event with inputs, outputs and facets
event := run.NewEvent(ol.EventTypeRunning).
    WithInputs(
        ol.NewInputElement("users", "postgres://db/schema"),
        ol.NewInputElement("orders", "postgres://db/schema"),
    ).
    WithOutputs(
        ol.NewOutputElement("user_orders", "s3://bucket/warehouse"),
    )
_, _ = run.Emit(ctx, event)

// If something goes wrong
if err := doWork(); err != nil {
    run.RecordError(err) // Marks run as failed, Finish() will emit FAIL
    return err
}
```

#### 2. Manual Event Construction (For fine-grained control)

For cases where you need full control over when events are emitted, you can construct events manually:

```go
producer := "https://github.com/your-org/your-integration/tree/v1.0.0"
runID := ol.NewRunID()

// Manually create and emit a START event
startEvent := ol.NewRunEvent(ol.EventTypeStart, runID, "my-job", producer).
    WithJobFacets(
        facets.NewJobType(producer, "BATCH", "SPARK"),
    ).
    WithInputs(
        ol.NewInputElement("source", "postgres://db/table"),
    )

// Emit whenever you're ready
if err := client.Emit(ctx, startEvent); err != nil {
    log.Fatal(err)
}

// ... do work ...

// Create and emit a COMPLETE event
completeEvent := ol.NewRunEvent(ol.EventTypeComplete, runID, "my-job", producer).
    WithRunFacets(
        facets.NewNominalTime(producer, time.Now()),
    ).
    WithOutputs(
        ol.NewOutputElement("target", "s3://bucket/data"),
    )

if err := client.Emit(ctx, completeEvent); err != nil {
    log.Fatal(err)
}
```

**When to use manual construction:**
- You need precise control over event timing
- Events are emitted across different parts of your application
- You're integrating with existing job orchestration that manages lifecycle
- You need to emit multiple events with the same runID from different processes

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
import (
    ol "github.com/OpenLineage/openlineage/client/go/pkg/openlineage"
    "github.com/OpenLineage/openlineage/client/go/pkg/facets"
)

// Create an event with facets
event := ol.NewRunEvent(ol.EventTypeStart, runID, "my-job", producer).
    WithRunFacets(
        facets.NewNominalTimeRunFacet(producer, time.Now()).
            WithNominalEndTime(time.Now().Add(time.Hour)),
        facets.NewProcessingEngineRunFacet(producer, "3.5.0").
            WithName("Apache Spark"),
    ).
    WithJobFacets(
        facets.NewSQLJobFacet(producer, "SELECT * FROM users"),
        facets.NewJobTypeJobFacet(producer, "SPARK", "BATCH").
            WithJobType("QUERY"),
    )
```

### Datasets

Add input and output datasets to your events:

```go
input := ol.NewInputElement("source_table", "postgres://host/db").
    WithFacets(
        facets.NewSchemaDatasetFacet(producer).WithFields([]facets.FieldElement{
            {Name: "id", Type: ol.Ptr("BIGINT")},
            {Name: "name", Type: ol.Ptr("VARCHAR")},
        }),
        facets.NewDatasourceDatasetFacet(producer).
            WithName("production-postgres").
            WithURI("postgres://prod-db.example.com:5432/analytics"),
    )

output := ol.NewOutputElement("target_table", "s3://bucket/warehouse").
    WithOutputFacets(
        facets.NewOutputStatisticsOutputDatasetFacet(producer).
            WithRowCount(500000).
            WithSize(262144000),
    )

event := ol.NewRunEvent(ol.EventTypeComplete, runID, "etl-job", producer).
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
    "github.com/OpenLineage/openlineage/client/go/pkg/transport"
)

// Using the dedicated constructor with options
ctx := context.Background()
t, err := transport.NewGCPLineageTransport(ctx, "my-gcp-project",
    transport.WithLocation("us-central1"),             // optional, default: us-central1
    transport.WithCredentialsFile("/path/to/sa.json"), // optional, uses ADC if not provided
)

// Or using the Config struct
producer := "https://github.com/your-org/your-integration/tree/v1.0.0"
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
client, err := ol.NewClient(producer, &cfg)
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
producer := "https://github.com/your-org/your-integration/tree/v1.0.0"
cfg := ol.ClientConfig{
    Disabled: true, // All Emit calls become no-ops
}
client, _ := ol.NewClient(producer, &cfg)
```

## Complete Examples

### Example 1: Using the Run Interface

```go
package main

import (
    "context"
    "log"

    ol "github.com/OpenLineage/openlineage/client/go/pkg/openlineage"
    "github.com/OpenLineage/openlineage/client/go/pkg/facets"
    "github.com/OpenLineage/openlineage/client/go/pkg/transport"
)

func main() {
    producer := "https://github.com/your-org/your-integration/tree/v1.0.0"

    client, err := ol.NewClient(producer, &ol.ClientConfig{
        Namespace: "analytics",
        Transport: transport.Config{
            Type: transport.TransportTypeHTTP,
            HTTP: transport.HTTPConfig{URL: "http://marquez:5000"},
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()

    // Start the main pipeline run — emits START
    ctx, run := client.StartRun(ctx, "daily-etl")
    defer run.Finish() // emits COMPLETE or FAIL

    // Emit a RUNNING event with inputs, outputs and job facets
    event := run.NewEvent(ol.EventTypeRunning).
        WithJobFacets(
            facets.NewJobTypeJobFacet(producer, "SPARK", "BATCH"),
            facets.NewSQLJobFacet(producer, "INSERT INTO analytics.summary SELECT * FROM staging.events"),
        ).
        WithInputs(
            ol.NewInputElement("events", "postgres://staging/public").
                WithFacets(
                    facets.NewSchemaDatasetFacet(producer).WithFields([]facets.FieldElement{
                        {Name: "event_id", Type: ol.Ptr("UUID")},
                        {Name: "event_type", Type: ol.Ptr("VARCHAR")},
                    }),
                ),
        ).
        WithOutputs(
            ol.NewOutputElement("summary", "s3://analytics/warehouse").
                WithOutputFacets(
                    facets.NewOutputStatisticsOutputDatasetFacet(producer).
                        WithRowCount(1000000).
                        WithSize(104857600),
                ),
        )

    if _, err := run.Emit(ctx, event); err != nil {
        run.RecordError(err)
        log.Printf("Pipeline failed: %v", err)
        return
    }

    if err := processData(ctx); err != nil {
        run.RecordError(err)
        log.Printf("Pipeline failed: %v", err)
    }
}

func processData(ctx context.Context) error {
    parent := ol.RunFromContext(ctx)

    ctx, childRun := parent.StartChild(ctx, "transform-step")
    defer childRun.Finish()

    _ = ctx
    return nil
}
```

### Example 2: Manual Event Construction

```go
package main

import (
    "context"
    "log"
    "time"

    ol "github.com/OpenLineage/openlineage/client/go/pkg/openlineage"
    "github.com/OpenLineage/openlineage/client/go/pkg/facets"
    "github.com/OpenLineage/openlineage/client/go/pkg/transport"
)

func main() {
    producer := "https://github.com/your-org/your-integration/tree/v1.0.0"
    
    // Create client
    client, err := ol.NewClient(producer, &ol.ClientConfig{
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
    runID := ol.NewRunID()
    jobName := "daily-etl"

    // START event — emitted at job start
    startEvent := ol.NewRunEvent(ol.EventTypeStart, runID, jobName, producer).
        WithJobFacets(
            facets.NewJobTypeJobFacet(producer, "BATCH", "SPARK"),
            facets.NewSQLJobFacet(producer, "INSERT INTO analytics.summary SELECT * FROM staging.events"),
        ).
        WithInputs(
            ol.NewInputElement("events", "postgres://staging/public").
                WithFacets(
                    facets.NewSchemaDatasetFacet(producer).WithFields([]facets.FieldElement{
                        {Name: "event_id", Type: ol.Ptr("UUID")},
                        {Name: "event_type", Type: ol.Ptr("VARCHAR")},
                        {Name: "timestamp", Type: ol.Ptr("TIMESTAMP")},
                    }),
                ),
        )

    if _, err := client.Emit(ctx, startEvent); err != nil {
        log.Fatal(err)
    }

    // Do the actual work
    startTime := time.Now()
    if err := processData(); err != nil {
        // FAIL event — emitted on error
        failEvent := ol.NewRunEvent(ol.EventTypeFail, runID, jobName, producer).
            WithRunFacets(
                facets.NewErrorMessageRunFacet(producer, err.Error(), "go"),
            )

        if _, err := client.Emit(ctx, failEvent); err != nil {
            log.Printf("Failed to emit fail event: %v", err)
        }
        log.Fatal(err)
    }

    // COMPLETE event — emitted on success
    completeEvent := ol.NewRunEvent(ol.EventTypeComplete, runID, jobName, producer).
        WithRunFacets(
            facets.NewNominalTimeRunFacet(producer, startTime).
                WithNominalEndTime(&[]time.Time{time.Now()}[0]),
        ).
        WithOutputs(
            ol.NewOutputElement("summary", "s3://analytics/warehouse").
                WithOutputFacets(
                    facets.NewOutputStatisticsOutputDatasetFacet(producer).
                        WithRowCount(1000000).
                        WithSize(104857600),
                ),
        )

    if _, err := client.Emit(ctx, completeEvent); err != nil {
        log.Fatal(err)
    }
}

func processData() error {
    // Processing logic here...
    return nil
}
```

## API Reference

### Client

| Method | Description |
|--------|-------------|
| `NewClient(producer, cfg)` | Create a new client with producer URL and configuration |
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
| `Emit(ctx, event)` | Emit an event using the run's client |
| `Finish(errs...)` | Complete the run (COMPLETE or FAIL) |
| `HasFailed()` | Returns true if RecordError was called |
| `RecordError(err)` | Record an error (marks run as failed) |

### Events

| Function | Description |
|----------|-------------|
| `NewRunEvent(type, runID, job, producer)` | Create a run event |
| `NewJobEvent(name, producer)` | Create a job event |
| `NewDatasetEvent(name, namespace, producer)` | Create a dataset event |

### Helpers

| Function | Description |
|----------|-------------|
| `NewRunID()` | Generate a new UUID v7 for use as a run ID (time-ordered) |
| `Ptr[T](v)` | Create a pointer to a value (useful for optional facet fields) |

## Contributing

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines.

## License

Apache License 2.0 - see [LICENSE](../../LICENSE) for details.


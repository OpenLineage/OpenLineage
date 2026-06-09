---
sidebar_position: 2
---

# Job type Job Facet

Facet to contain job properties like:
 * `processingType` which can be `BATCH`, `STREAMING`, or `SERVICE`,
 * `integration` which can be `SPARK|DBT|AIRFLOW|FLINK`,
 * `jobType` which can be `QUERY|COMMAND|DAG|TASK|JOB|MODEL`,
 * `emissionPattern` (optional) which describes how and what the job emits in its events.

The `processingType` and `emissionPattern` fields are complementary, and serve to describe in more detail the behaviour and expected lifecycle of a job, as well as what OpenLineage events is the job supposed to emit. 

## Emission Pattern

The `emissionPattern` object describes how and what the job emits in its events. It contains:

### Event Trigger

Defines when events are emitted:

* **`EVENT_BASED`** - Events emitted on lifecycle transitions
  * `START` when job begins
  * `COMPLETE`/`FAIL`/`ABORT` when job ends
  * `RUNNING` for progress updates (optional)
  
* **`PERIODIC`** - Events emitted at regular time intervals
  * `RUNNING` events are emitted on a schedule (e.g., every 5 minutes)
  * Other events related to the job lifecycle state can still be emitted, but it is expected that most lineage information and observability metrics will be captured in the periodic events.  

### Event Completeness

Defines what events contain:

* **`ACCUMULATIVE`** - Events may contain only partial information and the complete information can be collected by combining information from all the events emmited by a specific job run:
  * Individual events are more likely to contain partial rather then complete information
  * Consumers need to combine all events for a specif run to have complete information about the job run
  
* **`COMPLETE_SNAPSHOT`** - Events contain complete state for a specific time window
  * Each event is self-contained for its time period
  * Events can be processed independently
  * Example: Records processed in the last 5 minutes

### Window Duration

The `windowDuration` field (optional, integer) specifies the time window duration for periodic event emissions in seconds. Only applicable when `eventTrigger` is `PERIODIC`.

## Processing Type

The `processingType` field indicates the nature of the job and implicitly defines its lifecycle characteristics:

* **`BATCH`** - Finite jobs with clear start and end (batch ETL, bounded streams)
  * Jobs are expected to terminate naturally after completing their work
  * Emit `START` → [optional `RUNNING`] → `COMPLETE`/`FAIL`/`ABORT` events
  * Events are typically accumulative and emitted on lifecycle transitions
  
* **`STREAMING`** - Continuous jobs processing data streams
  * Jobs run indefinitely with no natural completion point
  * Process continuous data streams (e.g., Kafka, Flink, Spark Streaming)
  * Typically emit periodic events representing time-windowed snapshots
  * While `START`, `COMPLETE`, `ABORT`, or `FAIL` events can be emitted, they should not be relied upon as they might occur only occasionally when the streaming job is restarted

* **`SERVICE`** - Continuous long-running services
  * Jobs run indefinitely as background services or microservices
  * Similar to STREAMING but for general-purpose services rather than data stream processing
  * Typically emit periodic events with cumulative or snapshot metrics
  * While `START`, `COMPLETE`, `ABORT`, or `FAIL` events can be emitted, they should not be relied upon as they might occur only occasionally when the service is restarted

Unless specified otherwise, the job is assumed to be a `BATCH` job that emits `ACCUMULATIVE` events at the time of lifecycle transitions (`EVENT_BASED`).

## Basic Example

```json
{
    ...
    "job": {
        "facets": {
            "jobType": {
                "processingType": "BATCH",
                "integration": "SPARK",
                "jobType": "QUERY",
                "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
                "_schemaURL": "https://openlineage.io/spec/facets/2-0-4/JobTypeJobFacet.json"
            }
        }
	...
}
```

## Extended Examples

### Traditional Batch ETL Job

```json
{
    "job": {
        "facets": {
            "jobType": {
                "processingType": "BATCH",
                "integration": "SPARK",
                "jobType": "ETL",
                "emissionPattern": {
                    "eventTrigger": "EVENT_BASED",
                    "eventCompleteness": "ACCUMULATIVE"
                },
                "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
                "_schemaURL": "https://openlineage.io/spec/facets/2-0-4/JobTypeJobFacet.json"
            }
        }
    }
}
```

### Kafka Streams with 5-Minute Time Windows

```json
{
    "job": {
        "facets": {
            "jobType": {
                "processingType": "STREAMING",
                "integration": "KAFKA_STREAMS",
                "jobType": "STREAM_PROCESSOR",
                "emissionPattern": {
                    "eventTrigger": "PERIODIC",
                    "eventCompleteness": "COMPLETE_SNAPSHOT",
                    "windowDuration": 300
                },
                "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
                "_schemaURL": "https://openlineage.io/spec/facets/2-0-4/JobTypeJobFacet.json"
            }
        }
    }
}
```

### Long-Running Microservice

```json
{
    "job": {
        "facets": {
            "jobType": {
                "processingType": "SERVICE",
                "integration": "CUSTOM",
                "jobType": "MICROSERVICE",
                "emissionPattern": {
                    "eventTrigger": "PERIODIC",
                    "eventCompleteness": "COMPLETE_SNAPSHOT",
                    "windowDuration": 60
                },
                "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
                "_schemaURL": "https://openlineage.io/spec/facets/2-0-4/JobTypeJobFacet.json"
            }
        }
    }
}
```

## Integration-Specific Examples

 * Integration: `SPARK`
    * Processing type: `STREAMING`|`BATCH`
    * Job type: `JOB`|`COMMAND`
 * Integration: `AIRFLOW`
    * Processing type: `BATCH`
    * Job type: `DAG`|`TASK`
 * Integration: `DBT`
    * ProcessingType: `BATCH`
    * JobType: `PROJECT`|`MODEL`
 * Integration: `FLINK`
    * Processing type: `STREAMING`|`BATCH`
    * Job type: `JOB`

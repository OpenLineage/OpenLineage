# Proposal: Explicit Lineage — Facet-Based Approach

**Issue**: [OpenLineage#4359](https://github.com/OpenLineage/OpenLineage/issues/4359)  
**Status**: Draft  
**Date**: 2026-04-02

---

## Problem Statement

OpenLineage's current model captures lineage through two mechanisms: `inputs`/`outputs` arrays on Run and Job events, and `ColumnLineageDatasetFacet` (CLL) for column-level detail on individual outputs. Most integrations today (Spark, dbt, Flink) emit one output per job, so the implicit assumption that all inputs feed all outputs happens to be correct — dataset-level lineage works fine.

The problem appears when a single job reads and writes multiple independent datasets. The model infers the cartesian product of inputs x outputs: reading A,B and writing C,D produces four edges, even if the real flow is only A -> C and B -> D. In some cases, this makes it difficult to express a bulk ETL job processing 10 independent tables without artificially splitting it into separate jobs. CLL can resolve this at column granularity, but many integrations know "Table A feeds Table C" without column-level detail — and without CLL, the only fallback is the cartesian product.

Beyond this precision gap, the current model cannot express several common patterns:

- **Dataset-to-dataset derivation** — no mechanism for views, aliases, or manually documented lineage without an intermediate job
- **Mixed granularity** — CLL is all-or-nothing per output; no way to provide dataset-level lineage for some outputs and column-level for others in the same event
- **Job-to-job data flow** — no way to express that one job feeds data to another without an intermediate dataset (e.g., non-materialized views, stored procedures calling functions, in-memory data passing)

## Goals

Create a unified lineage structure that handles all the cases outlined above — dataset-level, column-level, mixed-level, dataset-to-dataset, and job-to-job — in a single model, eventually deprecating CLL. The structure should also be extensible to handle additional lineage types in the future without requiring new facets or model changes.

`type: JOB` is supported as a first-class entity type for three related cases: **sinks** (jobs that consume data without a tracked output), **generators** (jobs that produce data without a tracked input), and **job-to-job chains** (data flowing between jobs without an intermediate tracked dataset — e.g. stored procedures, non-materialized views, in-memory passing, streaming handoffs).

## Proposed Solution

We introduce three new facets — **LineageRunFacet**, **LineageJobFacet**, and **LineageDatasetFacet** — each carrying explicit lineage information and each restricted (by spec text, not JSON Schema) to a single event type:

| Facet | Entity it lives on | Allowed event type | Semantics |
|---|---|---|---|
| `LineageRunFacet` | Run | RunEvent only | Observed data flow during this specific run |
| `LineageJobFacet` | Job | JobEvent only | Declared/static data flow for this job definition |
| `LineageDatasetFacet` | Dataset | DatasetEvent only | Structural derivation of this dataset (views, aliases, manual docs) |

**Why the one-facet-per-event-type restriction?** If a RunEvent carried both a LineageRunFacet (on the run) and LineageDatasetFacet (on output datasets), the semantics would be ambiguous — which takes precedence? Do they merge? Rather than define complex interaction rules, we disallow the combination entirely. Each event type has exactly one place where lineage lives.

This restriction may be relaxed in a future version with clear type-of-lineage or hierarchy semantics defined (see [Future Evolution](#future-evolution)).

**LineageDatasetFacet** is the natural one. It sits on an output dataset and describes what feeds into that specific dataset — the same position as CLL. The target entity is implicit (the dataset the facet is attached to). This is a direct evolution of CLL.

**LineageRunFacet** and **LineageJobFacet** are the pragmatic ones. They sit on the run/job entity but describe relationships between other entities (datasets). This is unusual for a facet — normally a facet describes properties of the entity it's on. But it works because the event type provides the semantic frame: "during this run, data flowed this way" or "this job is designed to move data this way."


## Proposed Schema

### LineageDatasetFacet (DatasetFacet)

The natural facet. Lives on datasets in DatasetEvent. The target entity is implicit — it's the dataset the facet is attached to.

```json
"LineageDatasetFacet": {
  "type": "object",
  "description": "Explicit lineage for this dataset. Describes which source entities feed into it, at entity and/or column granularity. Supersedes ColumnLineageDatasetFacet.",
  "allOf": [{ "$ref": "#/$defs/DatasetFacet" }],
  "properties": {
    "inputs": {
      "type": "array",
      "description": "Dataset-level source inputs. When a source includes a 'field' property, it represents a dataset-wide operation (e.g., GROUP BY) where that source column affects the entire target dataset.",
      "items": {
        "$ref": "#/$defs/LineageInput"
      }
    },
    "fields": {
      "type": "object",
      "description": "Column-level lineage. Maps target field names to their source inputs.",
      "additionalProperties": {
        "$ref": "#/$defs/LineageFieldEntry"
      }
    }
  }
}
```

> Note: no `namespace`, `name`, or `type` on this facet — the target is the dataset it's attached to.


### LineageRunFacet (RunFacet)

Lives on the run object in RunEvent. Contains an array of `LineageEntry` objects, each identifying a target entity explicitly by namespace, name, and type, along with its source inputs and optional column-level detail.

```json
"LineageRunFacet": {
  "type": "object",
  "description": "Explicit lineage observed during this run. Describes data flow between entities at entity and/or column granularity.",
  "allOf": [{ "$ref": "#/$defs/RunFacet" }],
  "properties": {
    "entries": {
      "type": "array",
      "description": "Lineage entries describing data flow observed during this run.",
      "items": {
        "$ref": "#/$defs/LineageEntry"
      }
    }
  },
  "required": ["entries"]
}
```

### LineageJobFacet (JobFacet)

Lives on the job object in JobEvent. Same structure as LineageRunFacet.

```json
"LineageJobFacet": {
  "type": "object",
  "description": "Explicit lineage declared for this job definition. Describes designed/static data flow at entity and/or column granularity.",
  "allOf": [{ "$ref": "#/$defs/JobFacet" }],
  "properties": {
    "entries": {
      "type": "array",
      "description": "Lineage entries describing data flow designed for this job.",
      "items": {
        "$ref": "#/$defs/LineageEntry"
      }
    }
  },
  "required": ["entries"]
}
```

### Shared Types

#### LineageEntry (discriminated union)

Used by LineageRunFacet and LineageJobFacet. Each entry describes ONE target entity and what feeds into it. The `type` field discriminates between dataset targets and job targets:

```json
"LineageEntry": {
  "oneOf": [
    { "$ref": "#/$defs/LineageDatasetEntry" },
    { "$ref": "#/$defs/LineageJobEntry" }
  ]
}
```

The `enum` constraint on each subtype's `type` field (`"DATASET"` or `"JOB"`) ensures only one branch can match during validation.

#### LineageDatasetEntry

A lineage target that is a dataset. Supports both entity-level and column-level lineage:

```json
"LineageDatasetEntry": {
  "type": "object",
  "description": "Describes data flowing into a target dataset from source entities, at entity and/or column granularity.",
  "properties": {
    "namespace": {
      "type": "string",
      "description": "The namespace of the target dataset"
    },
    "name": {
      "type": "string",
      "description": "The name of the target dataset"
    },
    "type": {
      "type": "string",
      "enum": ["DATASET"],
      "description": "Must be DATASET"
    },
    "inputs": {
      "type": "array",
      "description": "Entity-level source inputs. Each item is a LineageInput (dataset or job).",
      "items": {
        "$ref": "#/$defs/LineageInput"
      }
    },
    "fields": {
      "type": "object",
      "description": "Column-level lineage. Maps target field names to their source inputs.",
      "additionalProperties": {
        "$ref": "#/$defs/LineageFieldEntry"
      }
    }
  },
  "required": ["namespace", "name", "type"]
}
```

#### LineageJobEntry

A lineage target that is a job. Used in two cases: (a) **sink** — a job consumes data without producing a tracked output dataset, with `LineageDatasetInput` sources; (b) **job-to-job chain** — a job receives data from another job with no intermediate tracked dataset, with `LineageJobInput` sources. Does not support column-level lineage (`fields`), but supports an optional `runId` to tie to a specific execution:

```json
"LineageJobEntry": {
  "type": "object",
  "description": "Describes data flowing into a target job. Used for sinks (jobs without tracked outputs) and as the target end of job-to-job chains.",
  "properties": {
    "namespace": {
      "type": "string",
      "description": "The namespace of the target job"
    },
    "name": {
      "type": "string",
      "description": "The name of the target job"
    },
    "type": {
      "type": "string",
      "enum": ["JOB"],
      "description": "Must be JOB"
    },
    "runId": {
      "type": "string",
      "format": "uuid",
      "description": "Optional. The specific run ID of the target job, when the lineage is tied to a particular execution."
    },
    "inputs": {
      "type": "array",
      "description": "Source inputs feeding into this job.",
      "items": {
        "$ref": "#/$defs/LineageInput"
      }
    }
  },
  "required": ["namespace", "name", "type"]
}
```

#### LineageFieldEntry

```json
"LineageFieldEntry": {
  "type": "object",
  "description": "Column-level lineage for a single target field.",
  "properties": {
    "inputs": {
      "type": "array",
      "description": "Source entities and/or fields that feed into this target field.",
      "items": {
        "$ref": "#/$defs/LineageInput"
      }
    }
  },
  "required": ["inputs"]
}
```

#### LineageInput (discriminated union)

A source entity that feeds data into a lineage target. The `type` field discriminates between dataset sources and job sources:

```json
"LineageInput": {
  "oneOf": [
    { "$ref": "#/$defs/LineageDatasetInput" },
    { "$ref": "#/$defs/LineageJobInput" }
  ]
}
```

Same pattern — the `enum` on each subtype's `type` field disambiguates.

#### LineageDatasetInput

A source input that is a dataset. Supports optional field reference and transformations:

```json
"LineageDatasetInput": {
  "type": "object",
  "description": "A source dataset that feeds data into a lineage target.",
  "properties": {
    "namespace": {
      "type": "string",
      "description": "The namespace of the source dataset"
    },
    "name": {
      "type": "string",
      "description": "The name of the source dataset"
    },
    "type": {
      "type": "string",
      "enum": ["DATASET"],
      "description": "Must be DATASET"
    },
    "field": {
      "type": "string",
      "description": "The specific field/column of the source dataset. Optional — when omitted at column level, means 'source column unknown'."
    },
    "transformations": {
      "type": "array",
      "description": "Transformations applied to the source data. Same structure as ColumnLineageDatasetFacet.",
      "items": {
        "$ref": "#/$defs/LineageTransformation"
      }
    }
  },
  "required": ["namespace", "name", "type"]
}
```

#### LineageJobInput

A source input that is a job. Used in two cases: (a) **generator** — when a `DATASET` target is produced by an upstream job with no tracked input dataset; (b) **job-to-job chain** — as the source end of a `JOB → JOB` edge. Supports an optional `runId` to tie to a specific execution:

```json
"LineageJobInput": {
  "type": "object",
  "description": "A source job that feeds data into a lineage target. Used when data comes from another job without an intermediate tracked dataset.",
  "properties": {
    "namespace": {
      "type": "string",
      "description": "The namespace of the source job"
    },
    "name": {
      "type": "string",
      "description": "The name of the source job"
    },
    "type": {
      "type": "string",
      "enum": ["JOB"],
      "description": "Must be JOB"
    },
    "runId": {
      "type": "string",
      "format": "uuid",
      "description": "Optional. The specific run ID of the source job, when the lineage is tied to a particular execution."
    }
  },
  "required": ["namespace", "name", "type"]
}
```

#### LineageTransformation

Reuses the existing CLL transformation structure:

```json
"LineageTransformation": {
  "type": "object",
  "properties": {
    "type": {
      "type": "string",
      "description": "DIRECT or INDIRECT"
    },
    "subtype": {
      "type": "string",
      "description": "E.g., IDENTITY, AGGREGATION, FILTER, JOIN, etc."
    },
    "description": {
      "type": "string"
    },
    "masking": {
      "type": "boolean"
    }
  },
  "required": ["type"]
}
```

## `type: JOB` Lineage: Sinks, Generators, and Job-to-Job Chains

The current OpenLineage model assumes every lineage edge connects **dataset to dataset**, with a job in the middle. Three common cases break that assumption — all three are different faces of the same `type: JOB` mechanism: sometimes the job is the lineage target, sometimes the source, sometimes both.

### Sinks: jobs that consume data without a tracked output

A **sink** is a job that reads data but doesn't write to any tracked dataset — for example, a fraud detection job that reads a `transactions` table, runs validation, and sends alerts (not modeled as a dataset). In the current spec this produces `inputs: [transactions], outputs: []`. While the consumer can infer "transactions feeds fraud_detector" from the event's job field, that information is implicit — it lives outside the lineage facet. The explicit lineage facet aspires to be the single source of truth (see [Precedence Rules](#precedence-rules)); it cannot fulfill that role if sink relationships are only knowable by joining the facet with `inputs`/`outputs`.

With `type: JOB` as a lineage **target** (`LineageJobEntry`), the sink edge is expressed inside the facet:

```
transactions (DATASET) ──> fraud_detector (JOB)
```

### Generators: jobs that produce data without a tracked input

A **generator** is the inverse: a job that produces data without reading from any tracked dataset — for example, an extract task that pulls from an external API and passes data in-memory to the next processing step. With `type: JOB` as a lineage **source** (`LineageJobInput`), we can express: "the data flowing into this dataset came from the extract_task job":

```
extract_task (JOB) ──> transformed_data (DATASET)
```

### Job-to-job chains: data flow without an intermediate tracked dataset

A **job-to-job chain** is data flowing from one job directly into another with no intermediate tracked dataset. The intermediate may be ephemeral (in-memory, a stream), implicit (a stored procedure invoking a function), or simply not materialized (a non-materialized view referenced by another query). Concrete cases:

- **Stored procedure chains** — `Table → Function → Stored Proc → Table`. The function and stored procedure pass data between themselves without materializing it.
- **Non-materialized views** referenced by downstream jobs — the view's defining job feeds the consuming job directly.
- **In-memory passing in orchestrators** — Airflow XCom, Dagster IO managers, Spark jobs that hand off DataFrames in shared context.
- **Streaming handoffs** — a Flink job emitting to a sink consumed by another Flink job through a shared topic that the user does not model as a dataset.

`LineageJobEntry` accepting `LineageJobInput` expresses this directly:

```
dag_a.task_3 (JOB) ──> dag_b.task_1 (JOB)
```

Either or both jobs may live in a different namespace from the event's own job, so the explicit `(namespace, name)` on `LineageJobEntry` and `LineageJobInput` is load-bearing — these chains cannot be reconstructed from the event's surrounding job field alone.

`runId` is OPTIONAL on both ends. On a `JobEvent`, it should be omitted: declared lineage describes the job definition, not a specific execution. On a `RunEvent`, it MAY be populated to bind the chain to specific upstream/downstream executions when the producer can identify them (e.g. when an orchestrator knows which run of `dag_a.task_3` fed this run of `dag_b.task_1`).

**Distinction from existing concepts.** Job-to-job lineage expresses **data flow**, not control flow or containment:

- `JobDependenciesRunFacet` expresses scheduling / control-flow dependencies — Job A runs Monday, Job B runs Tuesday because the schedule says so, regardless of whether any data passes between them. It remains the home for control flow.
- `ParentRunFacet` expresses containment — task X is part of DAG Y. Job-to-job lineage between sibling tasks within the same DAG is orthogonal to that hierarchy.

`JobEvent` is the natural home for declared job-to-job lineage ("this job is designed to feed data to that job"); `RunEvent` carries observed job-to-job lineage when a runtime integration can identify both ends of the chain.

### Why one mechanism for all three cases

A reasonable counter-question: do we need `type: JOB` at all? Sinks and generators are already implicit in the event's `inputs`/`outputs` arrays — couldn't a flag (`generated: true`, or simply `inputs: []` with no JOB type) express them?

It could, if sinks and generators were the only `type: JOB` cases. They are not. Once job-to-job chains are in scope, the JOB-type identity is irreducible:

- A chain involves two jobs that may both differ from the event's job, and may live in different namespaces from each other and from the event. There is no way to flag this with `generated: true` — the spec needs `(namespace, name)` for both ends.
- Once that machinery exists for chains, using a different mechanism for sinks and generators would split a single semantic concept ("a job participates in lineage") into three special cases, requiring three parser branches and three sets of consumer rules.

The early working group discussion explicitly flagged that having JOB sometimes implicit (sinks/generators) and sometimes explicit (chains) "causes inconsistency and ambiguity" and recommended consistent explicit JOB. We adopt that recommendation: one mechanism, one shape, three uses.

This also keeps the lineage facet self-contained: consumers can derive every lineage edge — dataset-to-dataset, sink, generator, chain, column-level — from the facet alone, without joining against `inputs`/`outputs`.

## Granularity Matrix

The combination of target type, source type, and field presence on either end enables all granularity levels in one table:

| Target type | Source type | Source field? | Meaning | Example |
|---|---|---|---|---|
| DATASET (entity-level) | DATASET | No | Dataset-level lineage | `output_table <- input_table` |
| DATASET (entity-level) | DATASET | Yes | Dataset-wide operation (e.g., GROUP BY) | `output_table <- input.group_by_col` |
| DATASET (per field) | DATASET | Yes | Column-level lineage | `output.total <- input.amount` |
| DATASET (per field) | DATASET | No | Partial column lineage (source column unknown) | `output.region <- lookup_table` |
| DATASET | JOB | — | Generator: dataset produced by upstream job | `output_table <- extract_task` |
| JOB | DATASET | No | Sink: job consumes entire dataset | `fraud_detector <- transactions` |
| JOB | DATASET | Yes | Sink: job consumes specific columns | `fraud_detector <- transactions.amount` |
| JOB | JOB | — | Job-to-job chain (no intermediate dataset) | `dag_b.task_1 <- dag_a.task_3` |

## Examples

### 1. Dataset-level lineage on RunEvent (solves #4359)

ETL job reads A,B and writes C,D. Only A->C and B->D are real edges. LineageRunFacet on the run:

```json
{
  "eventType": "COMPLETE",
  "run": {
    "runId": "abc-123",
    "facets": {
      "lineage": {
        "_producer": "https://example.com/etl",
        "_schemaURL": "https://openlineage.io/spec/facets/LineageRunFacet.json",
        "entries": [
          { "namespace": "postgresql://warehouse:5432", "name": "table_c", "type": "DATASET",
            "inputs": [{ "namespace": "postgresql://warehouse:5432", "name": "table_a", "type": "DATASET" }] },
          { "namespace": "postgresql://warehouse:5432", "name": "table_d", "type": "DATASET",
            "inputs": [{ "namespace": "postgresql://warehouse:5432", "name": "table_b", "type": "DATASET" }] }
        ]
      }
    }
  },
  "job": { "namespace": "http://etl-server:8080", "name": "bulk_etl" },
  "inputs": [
    { "namespace": "postgresql://warehouse:5432", "name": "table_a" },
    { "namespace": "postgresql://warehouse:5432", "name": "table_b" }
  ],
  "outputs": [
    { "namespace": "postgresql://warehouse:5432", "name": "table_c" },
    { "namespace": "postgresql://warehouse:5432", "name": "table_d" }
  ]
}
```

`inputs`/`outputs` remain as the carrier for dataset facets and as the fallback for older consumers. The LineageRunFacet defines the actual data flow for newer consumers.

### 2. Column-level lineage on RunEvent (subsumes CLL)

```json
"run": {
  "runId": "abc-123",
  "facets": {
    "lineage": {
      "_producer": "...",
      "_schemaURL": "...",
      "entries": [
        { "namespace": "postgresql://analytics:5432", "name": "output", "type": "DATASET",
          "fields": {
            "total": {
              "inputs": [
                { "namespace": "postgresql://analytics:5432", "name": "input", "type": "DATASET", "field": "amount",
                  "transformations": [{ "type": "INDIRECT", "subtype": "AGGREGATION" }] }
              ]
            }
          }
        }
      ]
    }
  }
}
```

### 3. Mixed granularity (dataset + column + dataset-wide ops)

```json
"entries": [
  { "namespace": "postgresql://analytics:5432", "name": "output_X", "type": "DATASET",
    "inputs": [
      { "namespace": "postgresql://analytics:5432", "name": "input_A", "type": "DATASET" },
      { "namespace": "postgresql://analytics:5432", "name": "input_A", "type": "DATASET", "field": "group_by_col",
        "transformations": [{ "type": "INDIRECT", "subtype": "GROUP_BY" }] }
    ],
    "fields": {
      "col1": {
        "inputs": [
          { "namespace": "postgresql://analytics:5432", "name": "input_A", "type": "DATASET", "field": "col_a" }
        ]
      },
      "col2": {
        "inputs": [
          { "namespace": "postgresql://analytics:5432", "name": "input_A", "type": "DATASET" }
        ]
      }
    }
  }
]
```

This single entry combines:
- **Entity-level**: output_X comes from input_A
- **Dataset-wide op**: group_by_col affects entire output
- **Column mapping**: col_a -> col1
- **Partial info**: col2 comes from input_A (source column unknown)

### 4. Dataset-to-dataset (job-less, on DatasetEvent)

A VIEW derives from base tables. No job involved. LineageDatasetFacet on the dataset:

```json
{
  "eventType": "COMPLETE",
  "eventTime": "2026-03-25T10:00:00.000Z",
  "dataset": {
    "namespace": "postgresql://warehouse:5432",
    "name": "public.customer_view",
    "facets": {
      "lineage": {
        "_producer": "https://example.com/catalog",
        "_schemaURL": "https://openlineage.io/spec/facets/LineageDatasetFacet.json",
        "inputs": [
          { "namespace": "postgresql://warehouse:5432", "name": "public.customers", "type": "DATASET" },
          { "namespace": "postgresql://warehouse:5432", "name": "public.orders", "type": "DATASET" }
        ]
      }
    }
  }
}
```

> Note: no `namespace`/`name`/`type` on the facet itself — the target is implicit from the dataset it's attached to.

### 5. Static job lineage (on JobEvent)

A catalog declares what a job is designed to do. LineageJobFacet on the job:

```json
{
  "job": {
    "namespace": "http://airflow:8080",
    "name": "daily_etl",
    "facets": {
      "lineage": {
        "_producer": "https://example.com/catalog",
        "_schemaURL": "https://openlineage.io/spec/facets/LineageJobFacet.json",
        "entries": [
          { "namespace": "postgresql://warehouse:5432", "name": "output_table", "type": "DATASET",
            "inputs": [
              { "namespace": "postgresql://warehouse:5432", "name": "source_table", "type": "DATASET" }
            ]
          }
        ]
      }
    }
  },
  "inputs": [{ "namespace": "postgresql://warehouse:5432", "name": "source_table" }],
  "outputs": [{ "namespace": "postgresql://warehouse:5432", "name": "output_table" }]
}
```

### 6. Data sink — job consumes data without tracked output

A fraud detection job reads transactions and sends alerts, but doesn't write to any tracked dataset. The job itself is the lineage target:

```json
{
  "eventType": "COMPLETE",
  "run": {
    "runId": "abc-456",
    "facets": {
      "lineage": {
        "_producer": "https://example.com/validation",
        "_schemaURL": "https://openlineage.io/spec/facets/LineageRunFacet.json",
        "entries": [
          { "namespace": "validation", "name": "fraud_detector", "type": "JOB",
            "inputs": [
              { "namespace": "postgres://prod", "name": "transactions", "type": "DATASET" }
            ]
          }
        ]
      }
    }
  },
  "job": { "namespace": "validation", "name": "fraud_detector" },
  "inputs": [
    { "namespace": "postgres://prod", "name": "transactions" }
  ],
  "outputs": []
}
```

### 7. Data generator — job produces data without tracked input

An extract task pulls data from an external API (not modeled as a dataset) and writes to a table. The upstream job is the lineage source:

```json
{
  "eventType": "COMPLETE",
  "run": {
    "runId": "abc-789",
    "facets": {
      "lineage": {
        "_producer": "https://example.com/etl",
        "_schemaURL": "https://openlineage.io/spec/facets/LineageRunFacet.json",
        "entries": [
          { "namespace": "postgres://prod", "name": "extracted_data", "type": "DATASET",
            "inputs": [
              { "namespace": "airflow://prod", "name": "data_pipeline.extract_task", "type": "JOB" }
            ]
          }
        ]
      }
    }
  },
  "job": { "namespace": "airflow://prod", "name": "data_pipeline.extract_task" },
  "inputs": [],
  "outputs": [
    { "namespace": "postgres://prod", "name": "extracted_data" }
  ]
}
```

### 8. Job-to-job — declared chain on JobEvent

A stored procedure `dag_b.task_1` reads its input from upstream `dag_a.task_3` via a non-materialized handoff. No intermediate dataset exists. The catalog declares the design-time data flow on the JobEvent for `dag_b`:

```json
{
  "eventType": "COMPLETE",
  "job": {
    "namespace": "airflow://prod",
    "name": "dag_b",
    "facets": {
      "lineage": {
        "_producer": "https://example.com/catalog",
        "_schemaURL": "https://openlineage.io/spec/facets/LineageJobFacet.json",
        "entries": [
          { "namespace": "airflow://prod", "name": "dag_b.task_1", "type": "JOB",
            "inputs": [
              { "namespace": "airflow://prod", "name": "dag_a.task_3", "type": "JOB" }
            ]
          }
        ]
      }
    }
  },
  "inputs": [],
  "outputs": []
}
```

Note that `runId` is omitted on both ends — declared lineage describes the job definition, not a specific execution. The same shape would apply to a non-materialized view consumed by a downstream job, or to an Airflow task receiving data via XCom from an upstream task.

### 9. Job-to-job — observed chain on RunEvent

The same data flow as Example 8, observed at runtime by an integration that can identify both the upstream and downstream runs. `runId` is populated on each end, binding the chain to specific executions:

```json
{
  "eventType": "COMPLETE",
  "run": {
    "runId": "run-b-001",
    "facets": {
      "lineage": {
        "_producer": "https://example.com/airflow",
        "_schemaURL": "https://openlineage.io/spec/facets/LineageRunFacet.json",
        "entries": [
          { "namespace": "airflow://prod", "name": "dag_b.task_1", "type": "JOB",
            "runId": "run-b-001",
            "inputs": [
              { "namespace": "airflow://prod", "name": "dag_a.task_3", "type": "JOB",
                "runId": "run-a-042" }
            ]
          }
        ]
      }
    }
  },
  "job": { "namespace": "airflow://prod", "name": "dag_b.task_1" },
  "inputs": [],
  "outputs": []
}
```

### 10. Job-to-job — cross-namespace chain

A streaming job in a Flink cluster feeds an in-memory topic consumed by a job in a separate Beam pipeline. Neither job is in the namespace of the other, and the topic is not modeled as a dataset. The Beam pipeline emits the cross-namespace chain on its RunEvent:

```json
{
  "eventType": "COMPLETE",
  "run": {
    "runId": "beam-run-77",
    "facets": {
      "lineage": {
        "_producer": "https://example.com/beam",
        "_schemaURL": "https://openlineage.io/spec/facets/LineageRunFacet.json",
        "entries": [
          { "namespace": "beam://analytics", "name": "enrichment_pipeline", "type": "JOB",
            "runId": "beam-run-77",
            "inputs": [
              { "namespace": "flink://ingest", "name": "event_normalizer", "type": "JOB" }
            ]
          }
        ]
      }
    }
  },
  "job": { "namespace": "beam://analytics", "name": "enrichment_pipeline" },
  "inputs": [],
  "outputs": []
}
```

The source job (`flink://ingest/event_normalizer`) is in a different namespace from both the event's job and the target entry. Without explicit `(namespace, name)` on both ends, the chain could not be expressed.

### 11. Structured-to-unstructured — known source columns, no target schema

Reading specific columns from a structured table and writing to an unstructured file (e.g., a PDF). The target has no schema, so there is no `fields` map — but we can still express that the output came from specific source columns:

```json
{
  "eventType": "COMPLETE",
  "run": {
    "runId": "abc-012",
    "facets": {
      "lineage": {
        "_producer": "https://example.com/export",
        "_schemaURL": "https://openlineage.io/spec/facets/LineageRunFacet.json",
        "entries": [
          { "namespace": "file://", "name": "R.pdf", "type": "DATASET",
            "inputs": [
              { "namespace": "postgres://prod", "name": "X", "type": "DATASET", "field": "A" },
              { "namespace": "postgres://prod", "name": "X", "type": "DATASET", "field": "B" }
            ]
          }
        ]
      }
    }
  },
  "job": { "namespace": "analytics", "name": "event_processor" },
  "inputs": [
    { "namespace": "postgres://prod", "name": "X" }
  ],
  "outputs": [
    { "namespace": "file://", "name": "R.pdf" }
  ]
}
```

## Precedence Rules

1. **If a lineage facet is present on the event**, it is the complete picture of lineage for that event. Consumers should derive all lineage edges from the facet and should not infer additional edges from `inputs`/`outputs` arrays or CLL facets. The `inputs`/`outputs` arrays remain as dataset facet carriers and backward-compatibility fallback, but are not a lineage source when a lineage facet is present.
2. **If no lineage facet is present**: use CLL on output datasets if available, then fall back to cartesian product of inputs x outputs (current behavior unchanged).
3. **If both a lineage facet and CLL are present** for the same output dataset, the lineage facet takes precedence.

## Semantic Constraints

These constraints are documented in spec text rather than enforced by JSON Schema. They are intentionally conservative — motivated by keeping lineage cohesive within a single event and avoiding the need to define interaction/precedence rules between multiple lineage facets. Any of these restrictions may be relaxed in a future version if use cases emerge; the current goal is to keep the initial proposal simple and unambiguous.

1. **One lineage facet per event**: A single event MUST NOT carry more than one type of lineage facet. A RunEvent uses LineageRunFacet; a JobEvent uses LineageJobFacet; a DatasetEvent uses LineageDatasetFacet. Mixing them is forbidden.

2. **Event type restriction**: LineageRunFacet MUST only appear on RunEvent. LineageJobFacet MUST only appear on JobEvent. LineageDatasetFacet MUST only appear on DatasetEvent.

3. **Producers MUST include datasets referenced in lineage facets in the event's inputs/outputs arrays** when applicable. These arrays are the carrier for dataset facets and the fallback for older consumers.

4. **Absence of lineage facet**: When no lineage facet is present on an event, the existing behavior applies unchanged — lineage is derived from CLL on output datasets if available, otherwise from the cartesian product of inputs x outputs.

5. At least one of `inputs` or `fields` SHOULD be present on a LineageDatasetEntry (or on LineageDatasetFacet directly). For LineageJobEntry, `inputs` SHOULD be present.

6. An **empty `inputs: []` array is semantically meaningful**: "this entity has no upstream source" (distinct from the entity not appearing in lineage at all).

7. Lineage entries and inputs do NOT carry a `facets` property. Facets belong on the entities they describe (Dataset, Run, Job), not on lineage edges.

8. **On RunEvent**: LineageRunFacet follows existing OpenLineage **accumulative semantics** — each event adds to the lineage picture for the run. Consumers merge lineage entries across events for the same `runId`.

9. **On DatasetEvent and JobEvent**: lineage facets use **replace semantics** — the latest event's lineage is the complete picture for that entity.

10. **`type: JOB` semantics**: A `LineageJobEntry` (target) and `LineageJobInput` (source) MUST identify the job by `(namespace, name)`. Either or both jobs MAY differ from the event's own job and from each other's namespace (e.g., for cross-namespace job-to-job chains). `runId` is OPTIONAL: on `RunEvent` it MAY be populated to bind the edge to a specific execution; on `JobEvent` it SHOULD be omitted, since declared lineage is execution-agnostic. Job-to-job lineage expresses **data flow only**; control-flow / scheduling dependencies remain in `JobDependenciesRunFacet`, and containment remains in `ParentRunFacet`.

## Interaction with Existing Features

### ParentRunFacet (hierarchy)

Remains orthogonal. Hierarchy expresses containment and scheduling (task X is part of DAG Y). Lineage facets express data flow. A consumer combines both dimensions.

The optional `runId` on `LineageJobEntry` and `LineageJobInput` is also orthogonal to ParentRunFacet — it identifies which execution of a job the lineage edge refers to, not the parent-child relationship between runs.

### JobDependenciesRunFacet (control flow)

Remains as-is for scheduling and control flow dependencies. Lineage facets express DATA flow dependencies — including job-to-job data flow, see [`type: JOB` Lineage](#type-job-lineage-sinks-generators-and-job-to-job-chains). Spec prose documents the distinction: a job may depend on another job for scheduling (control flow) without having a data dependency, and vice versa. The two facets are complementary, not overlapping.

### inputs[] / outputs[] arrays

Remain as-is. They serve two purposes that lineage facets do not replace:

- **Dataset facet carrier**: Dataset facets (schema, ownership, data quality, etc.) are attached to InputDataset and OutputDataset objects. There is no mechanism to attach facets to entities referenced in lineage entries.
- **Backward compatibility**: Older consumers that don't understand lineage facets still get the full list of datasets involved via inputs/outputs.

When a lineage facet is present, these arrays are **not a lineage source**. Consumers should derive lineage exclusively from the facet. The arrays exist for the two purposes above only.

### ColumnLineageDatasetFacet (CLL)

LineageDatasetFacet supersedes CLL. It can express everything CLL expresses, plus dataset-level lineage and mixed granularity. During the transition period, both may coexist on the same event — the lineage facet takes precedence.

## Migration & Compatibility

### This is a non-breaking change

New facets are optional. Adding new facet types does not break existing producers, consumers, or validators — this is the standard extensibility mechanism in OpenLineage.

### Client-side automatic translation

The OpenLineage client libraries (Java, Python) should provide built-in bidirectional translation between the old model (inputs/outputs/CLL) and the new lineage facets, gated by a configuration option. This is the key mechanism that makes migration painless — neither producers nor consumers need to change simultaneously.

The translation happens transparently in the client library's event emission path, after the producer constructs the event but before it is sent to the transport.

### CLL <-> Lineage field mapping

**CLL -> Lineage (lossless):**

| CLL structure | Lineage facet equivalent |
|---|---|
| `fields.{col}.inputFields[i]` | `fields.{col}.inputs[i]` with `field` on source |
| `dataset[i]` (GROUP BY, FILTER) | Top-level `inputs[i]` with `field` on source |
| Target dataset identity | Implicit — same dataset the facet is on |

**Lineage -> CLL (lossy):**

| Lineage facet structure | CLL mapping | Loss |
|---|---|---|
| Entity-level inputs (no field on source) | dropped | CLL has no dataset-level concept |
| Entity-level inputs (field on source) | -> `dataset[]` array | None |
| `fields.{col}.inputs` (field on source) | -> `fields.{col}.inputFields` | None |
| `fields.{col}.inputs` (no field on source) | dropped | CLL requires field on source |

For LineageRunFacet/LineageJobFacet: extract entries by target dataset, generate a CLL facet per output dataset, attach to the corresponding output in `outputs[]`. Same lossy mapping as above.

### Forward translation: lineage facets -> inputs/outputs/CLL

When a producer emits lineage facets but the downstream consumer only understands the old model, the client automatically generates the old representation:

1. **Populate inputs/outputs**: Scan all LineageEntry objects. For each target dataset, add to `outputs[]` if not present. For each source dataset in inputs, add to `inputs[]` if not present.
2. **Generate CLL facets**: For each target dataset with a `fields` map, construct a ColumnLineageDatasetFacet.
3. **Attach CLL to output datasets**: The generated CLL facet is attached to the corresponding OutputDataset in `outputs[]`.

This translation is lossy — dataset-level inputs without a field, fields entries where source has no field, and dataset-to-dataset entries with no column detail cannot be represented in CLL.

### Reverse translation: inputs/outputs/CLL -> lineage facets

When a producer only emits the old model but the downstream consumer expects lineage facets:

1. **Output datasets with CLL**: Create a LineageEntry from each CLL facet.
2. **Output datasets without CLL**: Create a LineageEntry with inputs containing ALL input datasets (the cartesian product — making implicit inference explicit).
3. **Construct the facet**: Wrap all LineageEntry objects in a LineageRunFacet.

This translation is lossless — CLL is a strict subset of what lineage facets can express.

### Configuration

```
openlineage.lineage.compatibility = none | legacy | modern | both
```

| Mode | Producer emits | Client generates | Use case |
|---|---|---|---|
| `none` | Whatever it wants | Nothing | Full control, no magic |
| `legacy` | Lineage facets | inputs/outputs/CLL | New producer, old consumers |
| `modern` | inputs/outputs/CLL | Lineage facets | Old producer, new consumers |
| `both` | Either or both | Whichever is missing | Transition period — everything works |

**Default during transition**: `both`.

### Translation scope

Translation applies to RunEvent and JobEvent only. For DatasetEvent, the LineageDatasetFacet is the primary and only representation.

Translation is idempotent: if both representations are already present, the client does not overwrite them. Producer-provided data always takes precedence.

### Producer strategy

- **Immediately**: Producers that want explicit lineage can start emitting lineage facets. With `compatibility = legacy`, old consumers still work automatically.
- **No-effort upgrade**: Producers that haven't been updated yet can set `compatibility = modern` to get lineage facets auto-generated from their existing inputs/outputs/CLL.
- **Phase 2 (deprecation)**: Eventually, producers migrate to emitting lineage facets natively. CLL emission can be dropped once consumers have caught up.

The transition period should be generous — measured in years.

### Consumer strategy

Consumers should implement the following precedence: check lineage facets first, fall back to CLL, then fall back to inputs x outputs. Client-side translation on the producer side means consumers will typically find the representation they need already present.

## Future Evolution

A key advantage of the facet-based approach is that these three lineage facets form a natural expansion point for capabilities descoped from this initial version. Rather than introducing new facets for each new lineage capability, we extend the existing ones by adding new fields and relaxing constraints:

### Replacing JobDependenciesRunFacet

With job-to-job data flow now expressible in lineage facets, `JobDependenciesRunFacet` could eventually be deprecated in favor of expressing both data flow and control flow dependencies through a unified mechanism — once the spec is comfortable folding control flow into the lineage facets (e.g., via a transformation type or edge attribute distinguishing data dependencies from scheduling dependencies). For this version, the two facets remain complementary.

### Relaxing the one-facet-per-event-type restriction

Initially, we disallow mixing facet types. As the model matures and interaction semantics become well-understood, this restriction could be relaxed — for example, allowing LineageDatasetFacet on output datasets in a RunEvent alongside LineageRunFacet on the run, with clear merge/precedence rules.

When this restriction is relaxed, a **lineage-type attribute** (runtime vs static) may be needed to disambiguate the semantics of a lineage facet that appears outside its "natural" event type. For instance, a LineageDatasetFacet on an output dataset within a RunEvent could be either "this is what I observed during this run" or "this is the static definition of this dataset" — an explicit attribute would resolve the ambiguity.

### New entity types

Adding new entity types (e.g., `MODEL`, `DASHBOARD`, `PIPELINE`) requires adding a new subtype definition to the `oneOf` in `LineageEntry` and/or `LineageInput`, along with spec prose updates and client library support. This is a non-breaking schema change — existing entries remain valid.

**The principle is**: grow the existing facets rather than proliferate new ones. Each lineage facet is a home for lineage information scoped to its entity type. New capabilities land as new fields or relaxed constraints within the same facet, keeping the model simple and the number of moving parts small.

## Appendix: Alternative Considered — Top-Level Property

An earlier version of this proposal placed lineage as a top-level `lineage` array directly on `BaseEvent`, alongside `inputs` and `outputs`. This would make lineage a first-class structural element of the event model rather than a facet.

Working group members raised concerns that a new top-level property would change the fundamental shape of the event model and create two parallel ways to express the same information (top-level arrays vs. facets). Since OpenLineage already has facets as its standard extensibility mechanism, the group concluded that lineage information fits naturally within that mechanism — avoiding a model-level change while preserving identical semantics and capabilities. The trade-off is slightly deeper nesting in the JSON structure and the need for three facets (one per entity type) rather than a single top-level array.

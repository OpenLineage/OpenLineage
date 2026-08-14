# Explicit lineage facets: developer guide

**Status**: Experimental  
**Issue**: [OpenLineage#4359](https://github.com/OpenLineage/OpenLineage/issues/4359)

---

> Experimental
>
> This feature is experimental. It is a large change to how OpenLineage models
> lineage, so we are still actively looking for feedback. We may merge the code
> and the spec, and ship it as part of the OpenLineage libraries. We may also
> change it or drop the idea altogether. If you have a use case, an objection or
> a suggestion, please raise it on the issue.

---

This guide shows you how to declare data flow explicitly with the two proposed
lineage facets. It walks through the model, the shared types, and worked
examples for each pattern.

Read the [proposal](ExplicitLineageProposal.md) for the design rationale, the
[schema](ExplicitLineageSchema.md) for field-by-field definitions, and the
[examples](ExplicitLineageExamples.md) for the full set of event samples.

## What this solves

OpenLineage infers dataset-level lineage from the cartesian product of `inputs`
and `outputs` on a run. When a job reads A and B and writes C and D, the model
infers four edges: A to C, A to D, B to C, and B to D. If the real flow is only
A to C and B to D, half those edges are wrong.

For a bulk ETL job that processes 10 independent tables in one run, most of the
inferred graph is noise.

`ColumnLineageDatasetFacet` solves this at column granularity. But many
integrations know that table A feeds table C without knowing the column
mapping, and there is no way to state that dataset-level fact without also
claiming that A feeds D.

The explicit lineage facets let you state the exact edges. They also cover
patterns the current model cannot express at all: dataset-to-dataset derivation
with no job, job-to-job data flow with no intermediate dataset, and mixed
dataset- and column-level detail in one event.

## The two facets

You declare lineage with one of two facets. The event type decides which facet
you use and how a consumer reads it.

| Facet | Lives on | Event type | Semantics |
|---|---|---|---|
| `LineageJobFacet` | the job (`job.facets.lineage`) | RunEvent | data flow observed during this run, accumulative |
| `LineageJobFacet` | the job (`job.facets.lineage`) | JobEvent | declared, static data flow for this job, replace |
| `LineageDatasetFacet` | the dataset (`dataset.facets.lineage`) | DatasetEvent | structural derivation of this dataset, replace |

Lineage is a property of the job, not of each run, so there is no separate
run-level facet. `LineageJobFacet` is a job facet, and the `job` object appears
on both RunEvent and JobEvent, so the same facet serves both. The event type
tells a consumer whether to merge entries across events for a run (accumulative)
or take the latest event as the whole picture (replace).

`LineageDatasetFacet` sits on a dataset and describes what feeds into it. The
target is implicit: it is the dataset the facet is attached to. This is the same
position as `ColumnLineageDatasetFacet`, which it evolves.

### Which facet to use

Pick the facet from what you are doing:

- you instrument a running job, such as Spark, Airflow, dbt or Flink: put `LineageJobFacet` on the job in your RunEvent, this is the most common case
- you declare what a job is designed to do, from a catalog or metadata tool, with no specific run: put `LineageJobFacet` on the job in a JobEvent
- you describe how a dataset derives from other datasets with no job involved, such as a view, an alias or manual documentation: put `LineageDatasetFacet` on the dataset in a DatasetEvent

An event carries only one kind of lineage facet. You cannot mix
`LineageJobFacet` and `LineageDatasetFacet` in the same event.

## Building blocks

Both facets are built from the same shared types.

```
LineageJobFacet          — lineage for a job (RunEvent and JobEvent)
  └─ entries[]              — one entry per target entity

LineageDatasetFacet      — lineage for a dataset (DatasetEvent)
  ├─ inputs[]              — entity-level sources (LineageInput[])
  └─ fields{}              — column-level sources (field name → LineageFieldEntry)

LineageDatasetEntry      — a target that is a dataset
  ├─ namespace, name        — identity of the target dataset
  ├─ type = "DATASET"
  ├─ inputs[]               — entity-level sources (LineageInput[])
  └─ fields{}               — column-level sources (field name → LineageFieldEntry)

LineageJobEntry          — a target that is a job
  ├─ namespace, name        — identity of the target job, or omit both for the event's own job
  ├─ type = "JOB"
  ├─ runId                  — optional, binds the edge to a specific run
  └─ inputs[]               — entity-level sources (LineageInput[])

LineageFieldEntry        — column-level lineage for one target field
  └─ inputs[]               — sources feeding this field (LineageInput[])

LineageInput             — one source, either a dataset or a job
  ├─ LineageDatasetInput      namespace, name, type="DATASET", field?, transformations?
  └─ LineageJobInput          namespace?, name?, type="JOB", runId?, transformations?

LineageTransformation    — how a source was transformed
  ├─ type                   — DIRECT or INDIRECT
  ├─ subtype                — for example IDENTITY, AGGREGATION, FILTER, JOIN, GROUP_BY
  ├─ description            — human-readable text
  └─ masking                — whether the transformation masks data, such as hashing personal data
```

Lineage is always written from the target's point of view. An entry names a
target and lists the inputs that feed into it. It never lists the outputs the
target feeds. This holds for dataset targets and job targets alike, and it
matches how `ColumnLineageDatasetFacet` already works.

### Dataset sources and job sources

The `type` field discriminates a source or a target between a dataset and a job:

- `DATASET` names a table, file, topic or similar, this is the common case
- `JOB` names a job, used for job-to-job chains and for adding transformation detail to the event's own job

A `JOB` source or target identifies the job by `namespace` and `name`. Omitting
both means the event's own job. The next sections show when each form applies.

## Examples

The examples below cover each capability. The
[examples file](ExplicitLineageExamples.md) holds the full set as complete
events.

### Dataset-level lineage

A bulk ETL job reads `table_a` and `table_b` and writes `table_c` and
`table_d`. The real flow is only A to C and B to D. `LineageJobFacet` on the job
states the two edges:

```json
{
  "eventType": "COMPLETE",
  "run": { "runId": "abc-123" },
  "job": {
    "namespace": "http://etl-server:8080",
    "name": "bulk_etl",
    "facets": {
      "lineage": {
        "_producer": "https://example.com/etl",
        "_schemaURL": "https://openlineage.io/spec/facets/LineageJobFacet.json",
        "entries": [
          { "namespace": "postgresql://warehouse:5432", "name": "table_c", "type": "DATASET",
            "inputs": [{ "namespace": "postgresql://warehouse:5432", "name": "table_a", "type": "DATASET" }] },
          { "namespace": "postgresql://warehouse:5432", "name": "table_d", "type": "DATASET",
            "inputs": [{ "namespace": "postgresql://warehouse:5432", "name": "table_b", "type": "DATASET" }] }
        ]
      }
    }
  },
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

The facet has one entry per output dataset, and each names its own source. There
is no cartesian product. The `inputs` and `outputs` arrays stay in place: they
carry the dataset facets, such as schema and quality, and they let older
consumers still see every dataset involved.

### Column-level lineage

This is the capability `ColumnLineageDatasetFacet` provides, expressed in the
new model. The `total` column of `output` aggregates `amount` from `input`:

```json
"job": {
  "namespace": "http://etl-server:8080",
  "name": "analytics_etl",
  "facets": {
    "lineage": {
      "_producer": "...",
      "_schemaURL": "https://openlineage.io/spec/facets/LineageJobFacet.json",
      "entries": [
        { "namespace": "postgresql://analytics:5432", "name": "output", "type": "DATASET",
          "fields": {
            "total": {
              "inputs": [
                { "namespace": "postgresql://analytics:5432", "name": "input", "type": "DATASET", "field": "amount",
                  "transformations": [{ "type": "DIRECT", "subtype": "AGGREGATION" }] }
              ]
            }
          }
        }
      ]
    }
  }
}
```

### Mixed granularity

One entry can hold dataset-level, dataset-wide and column-level detail at the
same time. You provide what you know:

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

This one entry states four things:

- entity-level: `output_X` comes from `input_A`
- dataset-wide operation: `group_by_col` affects the whole output
- column mapping: `col_a` feeds `col1`
- partial detail: `col2` comes from `input_A`, but the source column is unknown

The granularity comes from two choices: whether the target is a whole dataset or
a named field, and whether the source names a field. The table below reads each
row as `target <- source`.

| Target | Source | Source field | Meaning | Example |
|---|---|---|---|---|
| dataset | dataset | no | dataset-level lineage | `output_table <- input_table` |
| dataset | dataset | yes | dataset-wide operation, such as GROUP BY | `output_table <- input.group_by_col` |
| dataset field | dataset | yes | column-level lineage | `output.total <- input.amount` |
| dataset field | dataset | no | partial column lineage, source column unknown | `output.region <- lookup_table` |
| dataset | job, identified | — | dataset produced from a named upstream job | `output_table <- dag_a.task_3` |
| dataset field | own job, no identity | — | field produced by the event's own job, with transformation detail | `extracted_data.total <- sum() by own job` |
| job | dataset | no | named target job consumes a whole dataset | `dag_b.task_1 <- transactions` |
| job | dataset | yes | named target job consumes specific columns | `dag_b.task_1 <- transactions.amount` |
| own job, no identity | dataset | — | dataset consumed by the event's own job, with transformation detail | `own job <- transactions (FILTER)` |
| job | job | — | job-to-job chain, no intermediate dataset | `dag_b.task_1 <- dag_a.task_3` |

### Dataset-to-dataset derivation, no job

A view derives from base tables with no job in the picture. Use
`LineageDatasetFacet` on the dataset in a DatasetEvent:

```json
{
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

The facet has no `namespace`, `name` or `type`. The target is the dataset it is
attached to. This is what makes it the natural facet: you describe the entity
the facet sits on.

### Declared job lineage

A catalog states what a job is designed to do, without running it. Use
`LineageJobFacet` on the job in a JobEvent:

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

A JobEvent uses replace semantics: the latest event is the complete declared
lineage for the job. The typical producers are data catalogs, metadata import
tools and manual documentation systems.

### Sinks and generators

A sink is a job that reads data but writes no tracked dataset, such as a fraud
detector that reads transactions and sends alerts. A generator is the inverse: a
job that produces data with no tracked input, such as an extract task that pulls
from an external API.

You do not need a lineage facet for the plain cases. Event membership already
carries them:

- an input dataset that no entry references is consumed by the event's own job
- an output dataset that no entry describes is produced by the event's own job

So a sink is just `inputs` with an empty `outputs`:

```json
{
  "eventType": "COMPLETE",
  "run": { "runId": "abc-456" },
  "job": { "namespace": "validation", "name": "fraud_detector" },
  "inputs": [{ "namespace": "postgres://prod", "name": "transactions" }],
  "outputs": []
}
```

This still establishes the edge `transactions (DATASET) -> fraud_detector (JOB)`.
The edge is implicit in the event boundary, not repeated in a facet. A generator
is the mirror image: empty `inputs`, one entry in `outputs`.

Adding a `LineageJobEntry` for the event's own job here would only restate what
the event already carries, so do not do it.

### Enriched generator: transformation detail on the event's own job

A generator often knows how it produced a column, even with no tracked upstream
dataset. Here an extract task derives `total` with a `sum()` over an untracked
API response.

To record that, a field's input names an identity-less `LineageJobInput`: a
`{ "type": "JOB" }` source with no `namespace` or `name`. It resolves to the
event's own job and carries the transformation. The entry's top-level `inputs`
stays `[]`, because there is still no tracked upstream dataset:

```json
{
  "eventType": "COMPLETE",
  "run": { "runId": "abc-789" },
  "job": {
    "namespace": "airflow://prod",
    "name": "data_pipeline.extract_task",
    "facets": {
      "lineage": {
        "_producer": "https://example.com/extract",
        "_schemaURL": "https://openlineage.io/spec/facets/LineageJobFacet.json",
        "entries": [
          { "namespace": "postgres://prod", "name": "extracted_data", "type": "DATASET",
            "inputs": [],
            "fields": {
              "total": {
                "inputs": [
                  { "type": "JOB", "transformations": [{ "type": "DIRECT", "subtype": "AGGREGATION", "description": "sum()" }] }
                ]
              }
            }
          }
        ]
      }
    }
  },
  "inputs": [],
  "outputs": [{ "namespace": "postgres://prod", "name": "extracted_data" }]
}
```

The implicit edge from the job to `extracted_data` is unchanged. The
identity-less input only adds the per-column transformation that event
membership cannot express. The sink case works the same way in reverse: an
identity-less `LineageJobEntry` adds transformation detail for data the job
consumes.

### Job-to-job chains

Some data flows through a job that you cannot recover from `inputs`, `outputs`
and the event's own job. The intermediate may be a non-materialized view, a
stored procedure calling a function, in-memory passing such as Airflow XCom, or a
streaming handoff through an unmodelled topic.

A `LineageJobEntry` with a `LineageJobInput` states direct job-to-job flow. On a
JobEvent it is the declared chain, with `runId` omitted:

```json
{
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

On a RunEvent, an integration that can identify the runs on each end populates
`runId` to bind the chain to specific executions. Either job may be in a
different namespace from the event's own job, so the explicit `namespace` and
`name` on both ends are load-bearing: these chains cannot be rebuilt from the
event's own job field.

## Semantic rules

### One lineage facet per event

An event carries lineage in one facet only. A RunEvent or JobEvent uses
`LineageJobFacet` on the job. A DatasetEvent uses `LineageDatasetFacet` on the
dataset. `LineageDatasetFacet` must not appear on a RunEvent or JobEvent, and
`LineageJobFacet` must not appear on a DatasetEvent.

### Keep populating inputs and outputs

Producers must keep populating `inputs` and `outputs`, and must include every
dataset referenced in a lineage facet. These arrays do three things a lineage
facet does not:

- carry the dataset facets, such as schema, ownership and quality, which attach to input and output datasets and not to lineage edges
- let older consumers see every dataset involved
- define the event-job boundary, so dangling inputs and outputs map to the event's own job

When a lineage facet is present, these arrays are not a dataset-to-dataset
lineage source. A consumer must not infer the cartesian product from them.

### Provide at least one of inputs or fields

An entry, or `LineageDatasetFacet` itself, should have at least one of `inputs`
or `fields`. An entry with neither is valid but carries no lineage.

### An empty inputs array is meaningful

`"inputs": []` states that the entity has no upstream source. This differs from
the entity not appearing in lineage at all, which falls back to the cartesian
product, and from `inputs` being absent, where the entry gives only column-level
detail through `fields`.

### Semantics follow the event type

The same `LineageJobFacet` reads differently depending on its event:

- on a RunEvent it is accumulative, each event adds to the run's picture and a consumer merges entries across events for the same `runId`, and it describes data flow observed during the run
- on a JobEvent it uses replace, the latest event is the complete declared lineage for the job
- `LineageDatasetFacet` on a DatasetEvent also uses replace, the latest event is the complete derivation of the dataset

### Job entries express data flow only

A `type: JOB` source or target expresses data flow. Scheduling and control-flow
dependencies stay in `JobDependenciesRunFacet`. Containment stays in
`ParentRunFacet`. Omitting `namespace` and `name` together means the event's own
job, and you may use that form only to add transformation detail, never to
restate an edge the event boundary already carries. Emit `namespace` and `name`
together or omit them together.

## Precedence rules for consumers

During the transition, one event may carry a lineage facet,
`ColumnLineageDatasetFacet` and `inputs`/`outputs` at once. Read them in this
order:

1. If a lineage facet is present, take it as the complete explicit lineage between tracked entities. Do not infer extra dataset-to-dataset edges from `inputs`/`outputs` or from `ColumnLineageDatasetFacet`.
2. Read the event's own job from `inputs` and `outputs`. A dangling input is consumed by the event's job, and a dangling output is produced by it. These boundary edges do not create a cartesian product.
3. If no lineage facet is present, use `ColumnLineageDatasetFacet` on output datasets where available, then fall back to the cartesian product of inputs and outputs.
4. If a lineage facet and `ColumnLineageDatasetFacet` both describe the same output dataset, the lineage facet wins.

## How this relates to existing facets

### ParentRunFacet

`ParentRunFacet` expresses containment and hierarchy: task X is part of DAG Y.
Lineage facets express data flow. The two are orthogonal, and a consumer
combines both. The optional `runId` on job entries and inputs identifies which
execution an edge refers to, not a parent-child link between runs.

### JobDependenciesRunFacet

`JobDependenciesRunFacet` expresses scheduling and control flow: task B waits for
task A. Lineage facets express data flow: task B receives data from table X. A
job can have one without the other, so the two facets are complementary.

### ColumnLineageDatasetFacet

`LineageDatasetFacet` supersedes `ColumnLineageDatasetFacet`. It expresses
everything the column-lineage facet does, plus dataset-level lineage and mixed
granularity. During the transition both may appear on one event, and the lineage
facet takes precedence.

## Migration and compatibility

Adding these facets is a non-breaking change. The facets are optional, so
existing producers, consumers and validators keep working.

The client libraries will translate between the old model
(`inputs`/`outputs`/`ColumnLineageDatasetFacet`) and the new facets, so
producers and consumers do not have to change at the same time. A configuration
option controls it:

```
openlineage.lineage.compatibility = none | legacy | modern | both
```

| Mode | Producer emits | Client generates | Use case |
|---|---|---|---|
| `none` | whatever it wants | nothing | full control |
| `legacy` | lineage facets | inputs, outputs, column lineage | new producer, old consumers |
| `modern` | inputs, outputs, column lineage | lineage facets | old producer, new consumers |
| `both` | either or both | whichever is missing | transition period, the default |

Forward translation to the old model is lossy. Dataset-level inputs with no
field, field entries whose source has no field, and identity-less job inputs
cannot be represented in `ColumnLineageDatasetFacet`. Reverse translation from
the old model is lossless, because column lineage is a subset of what the
lineage facets express. Translation applies to RunEvent and JobEvent only. For a
DatasetEvent, `LineageDatasetFacet` is the only representation.

The transition should be generous, measured in years. See the
[proposal](ExplicitLineageProposal.md) for the full translation mapping and the
producer and consumer strategies.

# Explicit Lineage: Examples

This file contains examples for [ExplicitLineageProposal.md](ExplicitLineageProposal.md). See [ExplicitLineageSchema.md](ExplicitLineageSchema.md) for the proposed schema.

## 1. Dataset-level lineage on RunEvent (solves #4359)

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

`inputs`/`outputs` remain as the carrier for dataset facets, as the fallback for older consumers, and as the event boundary for the job's direct input/output participation.

## 2. Column-level lineage on RunEvent (subsumes CLL)

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

## 3. Mixed granularity (dataset + column + dataset-wide ops)

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

## 4. Dataset-to-dataset (job-less, on DatasetEvent)

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

## 5. Static job lineage (on JobEvent)

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

## 6. Data sink — job consumes data without tracked output

A fraud detection job reads transactions and sends alerts, but doesn't write to any tracked dataset. No lineage facet entry is needed: the input dataset is consumed by the event's job because it appears in `inputs`, and there is no tracked output dataset for it to feed.

```json
{
  "eventType": "COMPLETE",
  "run": {
    "runId": "abc-456"
  },
  "job": { "namespace": "validation", "name": "fraud_detector" },
  "inputs": [
    { "namespace": "postgres://prod", "name": "transactions" }
  ],
  "outputs": []
}
```

Conceptually, the event still establishes:

```
transactions (DATASET) ──> fraud_detector (JOB)
```

That edge is implicit in the RunEvent boundary, not repeated in `LineageRunFacet`. 
OpenLineage consumer may use that to visualize the relationship.

## 7. Data generator — job produces data without tracked input

An extract task pulls data from an external API (not modeled as a dataset) and writes to a table. No lineage facet entry is needed: the output dataset is produced by the event's job because it appears in `outputs`, and there is no tracked input dataset identified for it.

```json
{
  "eventType": "COMPLETE",
  "run": {
    "runId": "abc-789"
  },
  "job": { "namespace": "airflow://prod", "name": "data_pipeline.extract_task" },
  "inputs": [],
  "outputs": [
    { "namespace": "postgres://prod", "name": "extracted_data" }
  ]
}
```

Conceptually, the event still establishes:

```
extract_task (JOB) ──> extracted_data (DATASET)
```

That edge is implicit in the RunEvent boundary, not repeated with a `LineageJobInput`.

## 8. Job-to-job — declared chain on JobEvent

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

## 9. Job-to-job — observed chain on RunEvent

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

## 10. Job-to-job — cross-namespace chain

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

## 11. Structured-to-unstructured — known source columns, no target schema

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

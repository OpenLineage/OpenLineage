# Proposal: Explicit Lineage — Facet-Based Approach

**Issue**: [OpenLineage#4359](https://github.com/OpenLineage/OpenLineage/issues/4359)  
**Status**: Draft  
**Date**: 2026-04-02

---

## Problem Statement

TODO: bit more positive sentiment here. We will support more use cases due to this proposal.

OpenLineage's current model captures lineage through two mechanisms: `inputs`/`outputs` arrays on Run and Job events, and `ColumnLineageDatasetFacet` (CLL - Column-Level Lineage) for column-level detail on individual outputs. Most integrations today (Spark, dbt, Flink) emit one output per job, so the implicit assumption that all inputs feed all outputs happens to be correct — dataset-level lineage works fine.

The problem appears when a single job reads and writes multiple independent datasets. The model infers the cartesian product of inputs x outputs: reading A,B and writing C,D produces four edges, even if the real flow is only A -> C and B -> D. In some cases, this makes it difficult to express a bulk ETL job processing 10 independent tables without artificially splitting it into separate jobs. CLL can resolve this at column granularity, but many integrations know "Table A feeds Table C" without column-level detail — and without CLL, the only fallback is the cartesian product.

Beyond this gap, the current model cannot express several common patterns:

- **Dataset-to-dataset derivation** — no mechanism for views, aliases, or manually documented lineage without an intermediate job
- **Job-to-job data flow** — no way to express that one job feeds data to another without an intermediate dataset (e.g., non-materialized views, stored procedures calling functions, in-memory data passing)
- **Mixed granularity** — CLL is all-or-nothing per output; no way to provide dataset-level lineage for some outputs and column-level for others in the same event.
- **Job-to-dataset and dataset-to-job data flows - TODO**

## Goals

Create a unified lineage structure that handles all the cases outlined above — dataset-level, column-level, mixed-level, dataset-to-dataset, and job-to-job — in a single model, eventually deprecating CLL. The structure should also be extensible to handle additional lineage types in the future without requiring new facets or model changes.

## Proposed Solution

The general idea is to model the relationships explicitly; rather than implicitly; following and expanding on the ColumnLevelLineageDatasetFacet.

We introduce three new facets — **LineageRunFacet**, **LineageJobFacet**, and **LineageDatasetFacet** — each carrying explicit lineage information and each restricted (by spec text, not JSON Schema) to a single event type:


| Facet                 | Entity it lives on | Allowed event type | Semantics                                                           |
| --------------------- | ------------------ | ------------------ | ------------------------------------------------------------------- |
| `LineageRunFacet`     | Run                | RunEvent only      | Observed data flow during this specific run                         |
| `LineageJobFacet`     | Job                | JobEvent only      | Declared/static data flow for this job definition                   |
| `LineageDatasetFacet` | Dataset            | DatasetEvent only  | Structural derivation of this dataset (views, aliases, manual docs) |


**Why the one-facet-per-event-type restriction?** If a RunEvent carried both a LineageRunFacet (on the run) and LineageDatasetFacet (on output datasets), the semantics would be ambiguous — which takes precedence? Do they merge? Rather than define complex interaction rules, we disallow the combination entirely. Each event type has exactly one place where lineage lives.

This restriction may be relaxed in a future version if we define the semantics of that behavior (see [Future Evolution](#future-evolution)).

**LineageDatasetFacet** is the natural one. It sits on an output dataset and describes what feeds into that specific dataset — the same position as CLL. The target entity is implicit (the dataset the facet is attached to). This is a direct evolution of CLL.

**LineageRunFacet** and **LineageJobFacet** are the pragmatic ones. They sit on the run/job entity but describe relationships between other entities (datasets). This is not the usual way for a facet — normally a facet describes properties of the entity it's on. But it works because the event type provides the frame on which the event is one: "during this run, data flowed this way" or "this job is designed to move data this way." This information is also used in the Sinks/Generators section below - we know they live in the context of some dataset.

## Proposed Schema

The proposed facet and shared type schemas are defined in more details in [ExplicitLineageSchema.md](ExplicitLineageSchema.md).

There are special cases that need additional explanation and defined semantics:

### Sinks: jobs that consume data without a tracked output

A **sink** is a job that reads data but doesn't write to any tracked dataset — for example, a fraud detection job that reads a `transactions` table, runs validation, and sends alerts (not modeled as a dataset). This remains represented as:

```
job: fraud_detector
inputs: [transactions]
outputs: []
```

The dataset-to-job relationship is implicit in the event: `transactions` is consumed by the event's job because it appears in `inputs` and does not feed any tracked output dataset in the lineage facet. We intentionally do not add a `LineageJobEntry` for `fraud_detector`; that would duplicate information already carried by the event's `job` field.

```
transactions (DATASET) ──> fraud_detector (JOB)
```

### Generators: jobs that produce data without a tracked input

A **generator** is the inverse: a job that produces data without reading from any tracked dataset — for example, an extract task that pulls from an external API and writes the result to a table. This remains represented as:

```
job: extract_task
inputs: []
outputs: [transformed_data]
```

The job-to-dataset relationship is implicit in the event boundary: `transformed_data` is produced by the event's job because it appears in `outputs` and no tracked input dataset is identified for it in the lineage facet. In the simplest case we do not add any lineage entry for `extract_task`; the source job is already the event's job.

```
extract_task (JOB) ──> transformed_data (DATASET)
```

**Enriching a generator with field-level / transformation detail.** A generator often knows *how* it produced individual columns even though there is no tracked upstream dataset (e.g. an extract task that derives `total` via `sum()` over an external API response). To capture this without reintroducing the job as a tracked entity, a `LineageDatasetEntry` for the output dataset MAY name an **identity-less `LineageJobInput`** — a `{ "type": "JOB" }` source with no `namespace`/`name`, which resolves to the event's own job — and attach `transformations` to it:

```json
{ "namespace": "postgres://prod", "name": "extracted_data", "type": "DATASET",
  "fields": {
    "total": {
      "inputs": [
        { "type": "JOB", "transformations": [{ "type": "INDIRECT", "subtype": "AGGREGATION", "description": "sum()" }] }
      ]
    }
  }
}
```

This does not restate the implicit job-to-dataset edge (that remains carried by event membership); it only adds the per-field transformation detail that event membership cannot express. The output's top-level `inputs` stays `[]` — there is still no tracked upstream dataset.

### Explicit job-involving chains: data flow without an intermediate tracked dataset

An explicit job-involving chain is data flowing through a job that cannot be recovered from the event's own `job` field and `inputs`/`outputs`. The intermediate may be ephemeral (in-memory, a stream), implicit (a stored procedure invoking a function), or simply not materialized (a non-materialized view referenced by another query). Concrete cases:

- **Stored procedure chains** — `Table → Function → Stored Proc → Table`. The function and stored procedure pass data between themselves without materializing it.
- **Non-materialized views** referenced by downstream jobs — the view's defining job feeds the consuming job directly.
- **In-memory passing in orchestrators** — Airflow XCom, Dagster IO managers, Spark jobs that hand off DataFrames in shared context.
- **Streaming handoffs** — a Flink job emitting to a sink consumed by another Flink job through a shared topic that the user does not model as a dataset.

`LineageJobEntry` accepting `LineageJobInput` expresses direct job-to-job flow:

```
dag_a.task_3 (JOB) ──> dag_b.task_1 (JOB)
```

TODO: explain that we standarize from the point of "output" - we are describing the current job's inputs

Either or both jobs may live in a different namespace from the event's own job, so the explicit `(namespace, name)` on `LineageJobEntry` and `LineageJobInput` is load-bearing — these chains cannot be reconstructed from the event's surrounding job field alone. The same applies to a chain that ends in a dataset after an explicit upstream job, such as `dag_a.task_3 (JOB) -> output_table (DATASET)`: the upstream job identity is not the event's own job and must remain expressible as a `LineageJobInput`.

`runId` is OPTIONAL on both ends. On a `JobEvent`, it should be omitted: declared lineage describes the job definition, not a specific execution. On a `RunEvent`, it MAY be populated to bind the chain to specific upstream/downstream executions when the producer can identify them (e.g. when an orchestrator knows which run of `dag_a.task_3` fed this run of `dag_b.task_1`).

### Unmodeled intermediate artifacts

Some pipelines pass data between jobs through intermediate artifacts that are real at runtime but intentionally not modeled as datasets. For example:

```
dataset_1 ─┐
dataset_2 ─┼─> job_1 ── temp file 1+2 ──> job_2 ──> dataset_4
dataset_3 ─┘        └─ temp file 3 ─────> job_2 ──> dataset_5
```

In this shape, `job_1` writes both temporary files and `job_2` reads selected files to produce different outputs. The temporary files shown in the diagram are runtime artifacts, not proposed schema entities. They should not be exposed as datasets if their names are unstable or too implementation-specific; doing so would create incoherent lineage.

Without modeling those intermediate artifacts, the lineage model can only preserve coarser facts:

- From `job_1`'s point of view: `job_2` depends on `dataset_1`, `dataset_2`, and `dataset_3`, or simply `job_2` depends on `job_1`.
- From `job_2`'s point of view: `dataset_4` depends on `job_1`, and `dataset_5` also depends on `job_1`.

What is lost is the partitioning of the handoff: that `dataset_4` depends on the data path through `dataset_1` and `dataset_2`, while `dataset_5` depends on the path through `dataset_3`. The proposed model cannot express that distinction unless the intermediate artifacts, or some stable abstraction for them, are represented as lineage entities.

**Distinction from existing concepts.** Job-to-job lineage expresses **data flow**, not control flow or containment:

- `JobDependenciesRunFacet` expresses scheduling / control-flow dependencies — Job A runs Monday, Job B runs Tuesday because the schedule says so, regardless of whether any data passes between them. It remains the home for control flow.
- `ParentRunFacet` expresses containment — task X is part of DAG Y. Job-to-job lineage between sibling tasks within the same DAG is orthogonal to that hierarchy.

## Granularity Matrix

TODO: This is not clear at all

The combination of target type, source type, and field presence on either end enables all granularity levels in one table:


| Target type            | Source type                   | Source field? | Meaning                                                                                   | Example                                    |
| ---------------------- | ----------------------------- | ------------- | ----------------------------------------------------------------------------------------- | ------------------------------------------ |
| DATASET (entity-level) | DATASET                       | No            | Dataset-level lineage                                                                     | `output_table <- input_table`              |
| DATASET (entity-level) | DATASET                       | Yes           | Dataset-wide operation (e.g., GROUP BY)                                                   | `output_table <- input.group_by_col`       |
| DATASET (per field)    | DATASET                       | Yes           | Column-level lineage                                                                      | `output.total <- input.amount`             |
| DATASET (per field)    | DATASET                       | No            | Partial column lineage (source column unknown)                                            | `output.region <- lookup_table`            |
| DATASET                | JOB (identified)              | —             | Dataset produced from an explicitly identified upstream job                               | `output_table <- dag_a.task_3`             |
| DATASET (per field)    | JOB (identity-less = own job) | —             | Field produced by the event's own job, with transformation detail and no tracked upstream | `extracted_data.total <- sum() by own job` |
| JOB                    | DATASET                       | No            | Explicit target job consumes entire dataset                                               | `dag_b.task_1 <- transactions`             |
| JOB                    | DATASET                       | Yes           | Explicit target job consumes specific columns                                             | `dag_b.task_1 <- transactions.amount`      |
| JOB                    | JOB                           | —             | Job-to-job chain (no intermediate dataset)                                                | `dag_b.task_1 <- dag_a.task_3`             |


The ordinary sink and generator relationships for the event's own job are outside this matrix because they are not represented as explicit facet entries. They are represented by event membership: dangling `inputs` are consumed by the event's job, and dangling `outputs` are produced by the event's job.

## Precedence Rules

TODO: Why are we talking about this here

1. **If a lineage facet is present on the event**, it is the complete picture of explicit lineage between tracked entities for that event. Consumers should derive dataset-to-dataset, column-level, and explicit job-involving edges from the facet and should not infer additional dataset-to-dataset edges from `inputs`/`outputs` arrays or CLL facets.
2. **The event's own job remains implicit**. Even when a lineage facet is present, `inputs`/`outputs` still define the boundary of the event's job. Input datasets that are not referenced by any lineage entry are consumed directly by the event's job. Output datasets that are not described by any lineage entry are produced directly by the event's job. These implicit job-boundary relationships do not create a cartesian product between inputs and outputs.
3. **If no lineage facet is present**: use CLL on output datasets if available, then fall back to cartesian product of inputs x outputs (current behavior unchanged).
4. **If both a lineage facet and CLL are present** for the same output dataset, the lineage facet takes precedence.

## Semantic Constraints

These constraints are documented in spec text rather than enforced by JSON Schema. They are intentionally conservative — motivated by keeping lineage cohesive within a single event and avoiding the need to define interaction/precedence rules between multiple lineage facets. Any of these restrictions may be relaxed in a future version if use cases emerge; the current goal is to keep the initial proposal simple and unambiguous.

1. **One lineage facet per event**: A single event MUST NOT carry more than one type of lineage facet. A RunEvent uses LineageRunFacet; a JobEvent uses LineageJobFacet; a DatasetEvent uses LineageDatasetFacet. Mixing them is forbidden.
2. **Event type restriction**: LineageRunFacet MUST only appear on RunEvent. LineageJobFacet MUST only appear on JobEvent. LineageDatasetFacet MUST only appear on DatasetEvent.

TODO: reconsider only LineageJobFacet vs LineageRunFacet. The lineage should be a relatively "static" property - not change every run. 

1. **Producers MUST include datasets referenced in lineage facets in the event's inputs/outputs arrays** when applicable. These arrays are the carrier for dataset facets, the fallback for older consumers, and the source of implicit event-job boundary relationships for dangling inputs/outputs.
2. **Absence of lineage facet**: When no lineage facet is present on an event, the existing behavior applies unchanged — lineage is derived from CLL on output datasets if available, otherwise from the cartesian product of inputs x outputs.
3. At least one of `inputs` or `fields` SHOULD be present on a LineageDatasetEntry (or on LineageDatasetFacet directly). For LineageJobEntry, `inputs` SHOULD be present.
4. An **empty `inputs: []` array is semantically meaningful**: "this entity has no upstream source" (distinct from the entity not appearing in lineage at all).
5. A **dangling input** is an input dataset in the event `inputs` array that is not referenced as a source by any lineage entry. It is interpreted as data consumed directly by the event's job without a tracked output dataset. A **dangling output** is an output dataset in the event `outputs` array that is not represented as a lineage target. It is interpreted as data produced directly by the event's job without a tracked input dataset.
6. Lineage entries and inputs do NOT carry a `facets` property. Facets belong on the entities they describe (Dataset, Run, Job), not on lineage edges.
7. **On RunEvent**: LineageRunFacet follows existing OpenLineage **accumulative semantics** — each event adds to the lineage picture for the run. Consumers merge lineage entries across events for the same `runId`.
8. **On DatasetEvent and JobEvent**: lineage facets use **replace semantics** — the latest event's lineage is the complete picture for that entity.

TODO: Semantics should follow event types - behave differently based on event, not facets

1. `**type: JOB` semantics**: A `LineageJobEntry` (target) MUST identify the job by `(namespace, name)`. A `LineageJobInput` (source) identifies the job by `(namespace, name)` when both are present; either job MAY differ from the event's own job and from other jobs in the same lineage chain (e.g., for cross-namespace job-to-job-to-dataset chains). `runId` is OPTIONAL: on `RunEvent` it MAY be populated to bind the edge to a specific execution; on `JobEvent` it SHOULD be omitted, since declared lineage is execution-agnostic. Job-involving lineage expresses **data flow only**; control-flow / scheduling dependencies remain in `JobDependenciesRunFacet`, and containment remains in `ParentRunFacet`. A `LineageJobInput` is **not** used to restate that the event's own job consumes event inputs or produces event outputs at dataset granularity — that remains implicit via event membership.
  **Identity-less `LineageJobInput` (the event's own job)**: A `LineageJobInput` MAY omit both `namespace` and `name`, in which case it resolves to the event's own job. This form exists only to attach **field-level and/or transformation detail** to data produced by the implicit job (the generator case) — detail that event membership alone cannot carry. It MUST NOT be used as a top-level entity-level input to restate the implicit dataset-level edge. A producer MUST emit `namespace` and `name` together or omit them together.

## Interaction with Existing Features

### ParentRunFacet (hierarchy)

Remains orthogonal. Hierarchy expresses containment and scheduling (task X is part of DAG Y). Lineage facets express data flow. A consumer combines both dimensions.

The optional `runId` on `LineageJobEntry` and `LineageJobInput` is also orthogonal to ParentRunFacet — it identifies which execution of a job the lineage edge refers to, not the parent-child relationship between runs.

### JobDependenciesRunFacet (control flow)

Remains as-is for scheduling and control flow dependencies. Lineage facets express DATA flow dependencies — including explicit job-involving data flow, see [Event Job Boundaries and `type: JOB` Lineage](#event-job-boundaries-and-type-job-lineage). Spec prose documents the distinction: a job may depend on another job for scheduling (control flow) without having a data dependency, and vice versa. The two facets are complementary, not overlapping.

### inputs[] / outputs[] arrays

Remain as-is. They serve three purposes that lineage facets do not replace:

- **Dataset facet carrier**: Dataset facets (schema, ownership, data quality, etc.) are attached to InputDataset and OutputDataset objects. There is no mechanism to attach facets to entities referenced in lineage entries.
- **Backward compatibility**: Older consumers that don't understand lineage facets still get the full list of datasets involved via inputs/outputs.
- **Implicit event-job boundary**: The event's own job is not repeated in the lineage facet. Inputs and outputs that are not connected by explicit lineage entries still describe what the event job consumed or produced.

When a lineage facet is present, these arrays are **not a dataset-to-dataset lineage source**. Consumers should not infer the cartesian product of inputs x outputs. They do, however, continue to define the event-job boundary: dangling inputs are consumed by the event's job, and dangling outputs are produced by the event's job.

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


| CLL structure                   | Lineage facet equivalent                        |
| ------------------------------- | ----------------------------------------------- |
| `fields.{col}.inputFields[i]`   | `fields.{col}.inputs[i]` with `field` on source |
| `dataset[i]` (GROUP BY, FILTER) | Top-level `inputs[i]` with `field` on source    |
| Target dataset identity         | Implicit — same dataset the facet is on         |


**Lineage -> CLL (lossy):**


| Lineage facet structure                                                | CLL mapping                   | Loss                                                                    |
| ---------------------------------------------------------------------- | ----------------------------- | ----------------------------------------------------------------------- |
| Entity-level inputs (no field on source)                               | dropped                       | CLL has no dataset-level concept                                        |
| Entity-level inputs (field on source)                                  | -> `dataset[]` array          | None                                                                    |
| `fields.{col}.inputs` (field on source)                                | -> `fields.{col}.inputFields` | None                                                                    |
| `fields.{col}.inputs` (no field on source)                             | dropped                       | CLL requires field on source                                            |
| `fields.{col}.inputs` (identity-less JOB source, transformations only) | dropped                       | CLL has no notion of a field produced by the job with no dataset source |


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
2. **Output datasets without CLL**: Create a LineageEntry with inputs containing ALL input datasets (the cartesian product — making implicit dataset-to-dataset inference explicit). If there are no input datasets, create the entry with `inputs: []` to state that the output has no tracked upstream dataset.
3. **Construct the facet**: Wrap all LineageEntry objects in a LineageRunFacet.

This translation is lossless — CLL is a strict subset of what lineage facets can express.

### Configuration

```
openlineage.lineage.compatibility = none | legacy | modern | both
```


| Mode     | Producer emits     | Client generates     | Use case                             |
| -------- | ------------------ | -------------------- | ------------------------------------ |
| `none`   | Whatever it wants  | Nothing              | Full control, no magic               |
| `legacy` | Lineage facets     | inputs/outputs/CLL   | New producer, old consumers          |
| `modern` | inputs/outputs/CLL | Lineage facets       | Old producer, new consumers          |
| `both`   | Either or both     | Whichever is missing | Transition period — everything works |


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

A key advantage of the facet-based approach is that these three lineage facets form a natural expansion point for capabilities descoped from this initial version. 
Rather than introducing new facets for each new lineage capability, we extend the existing ones by adding new fields and relaxing constraints:

### Replacing JobDependenciesRunFacet

With job-to-job data flow now expressible in lineage facets, `JobDependenciesRunFacet` could eventually be deprecated in favor of expressing both data flow and control flow dependencies through a unified mechanism.
To do that, the lineage facets would need to be able to express the dependency type. For example, via a transformation type or edge attribute distinguishing data dependencies from scheduling dependencies.
For this version, the two facets remain complementary.

### Relaxing the one-facet-per-event-type restriction

Initially, we disallow mixing facet types. As the model matures and interaction semantics become well-understood, this restriction could be relaxed — for example, allowing LineageDatasetFacet on output datasets in a RunEvent alongside LineageRunFacet on the run, with clear merge/precedence rules.

When this restriction is relaxed, a separate lineage-type attribute (runtime vs static) may be needed to disambiguate the semantics of a lineage facet that appears outside its "natural" event type. 
For instance, a LineageDatasetFacet on an output dataset within a RunEvent could be either "this is what I observed during this run" or "this is the static definition of this dataset" — an explicit attribute would resolve the ambiguity.

### New entity types

Adding new entity types (e.g., `MODEL`, `DASHBOARD`, `PIPELINE`) requires adding a new subtype definition to the relevant `oneOf` definitions, along with spec prose updates and client library support. 
This would be a non-breaking schema change — existing entries remain valid.

The principle here is to contain the lineage in the currently proposed facets rather than proliferate new ones - and duplicate the same information.
Each lineage facet is a home for lineage information scoped to its entity type. 
New capabilities land as new fields or relaxed constraints within the same facet, keeping the model simple and the number of moving parts small.

## Appendix: Alternative Considered — Top-Level Property

An earlier version of this proposal placed lineage as a top-level `lineage` array directly on `BaseEvent`, alongside `inputs` and `outputs`. This would make lineage a first-class structural element of the event model rather than a facet.

Working group members raised concerns that a new top-level property would change the fundamental shape of the event model and create two parallel ways to express the same information (top-level arrays vs. facets). Since OpenLineage already has facets as its standard extensibility mechanism, the group concluded that lineage information fits naturally within that mechanism — avoiding a model-level change while preserving identical semantics and capabilities. The trade-off is slightly deeper nesting in the JSON structure and the need for three facets (one per entity type) rather than a single top-level array.

## Examples

Examples are defined in [ExplicitLineageExamples.md](ExplicitLineageExamples.md).
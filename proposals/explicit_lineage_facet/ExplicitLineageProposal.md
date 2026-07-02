# Proposal: Explicit Lineage — Facet-Based Approach

**Issue**: [OpenLineage#4359](https://github.com/OpenLineage/OpenLineage/issues/4359)  
**Status**: Draft  
**Date**: 2026-04-02

---

## Problem Statement

This proposal lets OpenLineage express more lineage in one consistent model: multi-dataset jobs, dataset-to-dataset derivation, job-to-job data flow, and mixed dataset- and column-level detail. These are patterns producers see today but cannot represent. The rest of this section sets out what the current model does well and where it falls short.

OpenLineage's current model captures lineage through two mechanisms: `inputs`/`outputs` arrays on Run and Job events, and `ColumnLineageDatasetFacet` (CLL - Column-Level Lineage) for column-level detail on individual outputs. Most integrations today (Spark, dbt, Flink) emit one output per job, so the implicit assumption that all inputs feed all outputs happens to be correct — dataset-level lineage works fine.

The problem appears when a single job reads and writes multiple independent datasets. The model infers the cartesian product of inputs x outputs: reading A,B and writing C,D produces four edges, even if the real flow is only A -> C and B -> D. In some cases, this makes it difficult to express a bulk ETL job processing 10 independent tables without artificially splitting it into separate jobs. CLL can resolve this at column granularity, but many integrations know "Table A feeds Table C" without column-level detail — and without CLL, the only fallback is the cartesian product.

Beyond this gap, the current model cannot express several common patterns:

- **Dataset-to-dataset derivation** — no mechanism for views, aliases, or manually documented lineage without an intermediate job
- **Job-to-job data flow** — no way to express that one job feeds data to another without an intermediate dataset (e.g., non-materialized views, stored procedures calling functions, in-memory data passing)
- **Mixed granularity** — CLL is all-or-nothing per output; no way to provide dataset-level lineage for some outputs and column-level for others in the same event.
- **Transformation detail on a job's own production or consumption** — a job that produces a dataset with no tracked input (a generator) or consumes one with no tracked output (a sink) can only be stated as a plain edge. There is no way to record how the job derived each column or what transformation it applied.

## Goals

Create a unified lineage structure that handles all the cases outlined above — dataset-level, column-level, mixed-level, dataset-to-dataset, and job-to-job — in a single model, eventually deprecating CLL. The structure should also be extensible to handle additional lineage types in the future without requiring new facets or model changes.

## Proposed Solution

The general idea is to model the relationships explicitly; rather than implicitly; following and expanding on the ColumnLevelLineageDatasetFacet.

We introduce two new facets — `LineageJobFacet` and `LineageDatasetFacet`. The event type decides where lineage lives and how to read it:


| Event type   | Lineage facet         | Lives on    | Semantics                                                              |
| ------------ | --------------------- | ----------- | --------------------------------------------------------------------- |
| RunEvent     | `LineageJobFacet`     | the job     | Data flow observed during this run (accumulative)                     |
| JobEvent     | `LineageJobFacet`     | the job     | Declared, static data flow for this job definition (replace)          |
| DatasetEvent | `LineageDatasetFacet` | the dataset | Structural derivation of this dataset — views, aliases, manual (replace) |


Lineage is a property of the job, not of each individual run. So there is no separate run-level facet. `LineageJobFacet` is a JobFacet, and the `job` object is present on both RunEvent and JobEvent, so the same facet serves both event types. On a JobEvent it is the declared, static data flow. On a RunEvent it is the data flow observed during that run, and it MAY bind edges to specific executions through the optional `runId` on job-typed entries.

`LineageDatasetFacet` sits on a dataset and describes what feeds into it. The target is implicit — it is the dataset the facet is attached to. This is the same position as CLL, which it evolves.

`LineageJobFacet` is the less usual shape: it sits on the job but describes relationships between other entities (datasets and jobs), not properties of the job itself. This works because the event supplies the frame — "during this run, data flowed this way" or "this job is designed to move data this way."

**Why one lineage facet per event type?** A RunEvent could in principle carry both a `LineageJobFacet` on the job and a `LineageDatasetFacet` on its output datasets. The semantics would then be ambiguous — which takes precedence, and do they merge? Rather than define interaction rules, we disallow the combination. Lineage for a RunEvent or JobEvent lives only in `LineageJobFacet`; lineage for a DatasetEvent lives only in `LineageDatasetFacet`. This restriction may be relaxed in a future version once the interaction semantics are defined (see [Future Evolution](#future-evolution)).

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

Lineage is always expressed from the target's point of view. An entry names a target entity and lists the inputs that feed into it — never the outputs it feeds. So the chain above is recorded as an entry whose target is `dag_b.task_1`, with `dag_a.task_3` among its inputs. This matches how CLL already works, where a target dataset lists its input fields, and it keeps one orientation across both dataset and job targets.

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

One structure covers every granularity level, from dataset-level down to column-level. Each edge is set by three choices:

- the target — a whole dataset, a single dataset field, or a job
- the source — a dataset or a job
- whether the source names a specific field

Read each row as `target <- source`. The combinations below show what each one means.


| Target                        | Source                        | Source field? | Meaning                                                                                   | Example                                    |
| ----------------------------- | ----------------------------- | ------------- | ----------------------------------------------------------------------------------------- | ------------------------------------------ |
| Dataset                       | Dataset                       | No            | Dataset-level lineage                                                                     | `output_table <- input_table`              |
| Dataset                       | Dataset                       | Yes           | Dataset-wide operation (for example, GROUP BY)                                            | `output_table <- input.group_by_col`       |
| Dataset field                 | Dataset                       | Yes           | Column-level lineage                                                                      | `output.total <- input.amount`             |
| Dataset field                 | Dataset                       | No            | Partial column lineage (source column unknown)                                            | `output.region <- lookup_table`            |
| Dataset                       | Job (identified)              | —             | Dataset produced from an explicitly identified upstream job                               | `output_table <- dag_a.task_3`             |
| Dataset field                 | Own job (identity-less)       | —             | Field produced by the event's own job, with transformation detail and no tracked upstream (generator) | `extracted_data.total <- sum() by own job` |
| Job                           | Dataset                       | No            | Explicitly identified target job consumes a whole dataset                                 | `dag_b.task_1 <- transactions`             |
| Job                           | Dataset                       | Yes           | Explicitly identified target job consumes specific columns                                | `dag_b.task_1 <- transactions.amount`      |
| Own job (identity-less)       | Dataset                       | —             | Dataset consumed by the event's own job, with transformation detail and no tracked output (sink) | `own job <- transactions (FILTER)`         |
| Job                           | Job                           | —             | Job-to-job chain (no intermediate dataset)                                                | `dag_b.task_1 <- dag_a.task_3`             |


Plain sink and generator edges for the event's own job are not in this matrix, because they are not explicit facet entries. Event membership carries them: dangling `inputs` are consumed by the event's job, and dangling `outputs` are produced by it. The two identity-less rows above only add transformation detail that event membership cannot carry.

## Precedence Rules

During the transition, one event may carry a lineage facet, CLL, and `inputs`/`outputs` at the same time. These rules tell a consumer which representation to trust, and in what order, so the same event is never read two ways:

1. **If a lineage facet is present on the event**, it is the complete picture of explicit lineage between tracked entities for that event. Consumers should derive dataset-to-dataset, column-level, and explicit job-involving edges from the facet and should not infer additional dataset-to-dataset edges from `inputs`/`outputs` arrays or CLL facets.
2. **The event's own job remains implicit**. Even when a lineage facet is present, `inputs`/`outputs` still define the boundary of the event's job. Input datasets that are not referenced by any lineage entry are consumed directly by the event's job. Output datasets that are not described by any lineage entry are produced directly by the event's job. These implicit job-boundary relationships do not create a cartesian product between inputs and outputs.
3. **If no lineage facet is present**: use CLL on output datasets if available, then fall back to cartesian product of inputs x outputs (current behavior unchanged).
4. **If both a lineage facet and CLL are present** for the same output dataset, the lineage facet takes precedence.

## Semantic Constraints

These constraints are documented in spec text rather than enforced by JSON Schema. They are intentionally conservative — motivated by keeping lineage cohesive within a single event and avoiding the need to define interaction/precedence rules between multiple lineage facets. Any of these restrictions may be relaxed in a future version if use cases emerge; the current goal is to keep the initial proposal simple and unambiguous.

1. **One lineage facet per event type**: A RunEvent or JobEvent carries lineage in `LineageJobFacet` (on the job); a DatasetEvent carries it in `LineageDatasetFacet` (on the dataset). An event MUST NOT mix the two. `LineageDatasetFacet` MUST NOT appear on a RunEvent or JobEvent, and `LineageJobFacet` MUST NOT appear on a DatasetEvent.
2. **Producers MUST include datasets referenced in lineage facets in the event's inputs/outputs arrays** when applicable. These arrays are the carrier for dataset facets, the fallback for older consumers, and the source of implicit event-job boundary relationships for dangling inputs/outputs.
3. **Absence of lineage facet**: When no lineage facet is present on an event, the existing behavior applies unchanged — lineage is derived from CLL on output datasets if available, otherwise from the cartesian product of inputs x outputs.
4. At least one of `inputs` or `fields` SHOULD be present on a LineageDatasetEntry (or on LineageDatasetFacet directly). For LineageJobEntry, `inputs` SHOULD be present.
5. An **empty `inputs: []` array is semantically meaningful**: "this entity has no upstream source" (distinct from the entity not appearing in lineage at all).
6. A **dangling input** is an input dataset in the event `inputs` array that is not referenced as a source by any lineage entry. It is interpreted as data consumed directly by the event's job without a tracked output dataset. A **dangling output** is an output dataset in the event `outputs` array that is not represented as a lineage target. It is interpreted as data produced directly by the event's job without a tracked input dataset.
7. Lineage entries and inputs do NOT carry a `facets` property. Facets belong on the entities they describe (Dataset, Run, Job), not on lineage edges.
8. **Semantics follow the event type, not the facet**. The same `LineageJobFacet` is read differently depending on the event it rides on:
    - On a RunEvent, it follows existing OpenLineage accumulative semantics — each event adds to the lineage picture for the run, and consumers merge entries across events for the same `runId`. It describes data flow observed during the run.
    - On a JobEvent, it uses replace semantics — the latest event is the complete declared lineage for the job.
    - `LineageDatasetFacet` on a DatasetEvent also uses replace semantics — the latest event is the complete derivation of the dataset.

9. **`type: JOB` semantics**: A `LineageJobEntry` (target) and a `LineageJobInput` (source) both identify a job by `(namespace, name)`. When both are present, the job MAY differ from the event's own job and from other jobs in the same chain, which is what makes cross-namespace job-to-job-to-dataset chains expressible. `runId` is OPTIONAL: on a RunEvent it MAY bind the edge to a specific execution; on a JobEvent it SHOULD be omitted, since declared lineage is execution-agnostic. Job-involving lineage expresses data flow only. Scheduling and control-flow dependencies remain in `JobDependenciesRunFacet`, and containment remains in `ParentRunFacet`.
10. **Identity-less job entries and inputs (the event's own job)**: A `LineageJobEntry` or `LineageJobInput` MAY omit both `namespace` and `name`, in which case it resolves to the event's own job, with its identity and run taken from the top-level event. This form exists only to attach transformation and field detail that event membership cannot carry: an identity-less `LineageJobInput` covers the generator case (data the job produces), and an identity-less `LineageJobEntry` covers the sink case (data the job consumes). It MUST NOT restate the implicit dataset-level edge that event membership already carries. A producer MUST emit `namespace` and `name` together or omit them together.

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


For `LineageJobFacet`: extract entries by target dataset, generate a CLL facet per output dataset, attach to the corresponding output in `outputs[]`. Same lossy mapping as above.

### Forward translation: lineage facets -> inputs/outputs/CLL

When a producer emits lineage facets but the downstream consumer only understands the old model, the client automatically generates the old representation:

1. **Populate inputs/outputs**: Scan all entries in the lineage facet. For each target dataset, add to `outputs[]` if not present. For each source dataset in inputs, add to `inputs[]` if not present.
2. **Generate CLL facets**: For each target dataset with a `fields` map, construct a ColumnLineageDatasetFacet.
3. **Attach CLL to output datasets**: The generated CLL facet is attached to the corresponding OutputDataset in `outputs[]`.

This translation is lossy — dataset-level inputs without a field, fields entries where source has no field, and dataset-to-dataset entries with no column detail cannot be represented in CLL.

### Reverse translation: inputs/outputs/CLL -> lineage facets

When a producer only emits the old model but the downstream consumer expects lineage facets:

1. **Output datasets with CLL**: Create a lineage entry from each CLL facet.
2. **Output datasets without CLL**: Create a lineage entry with inputs containing ALL input datasets (the cartesian product — making implicit dataset-to-dataset inference explicit). If there are no input datasets, create the entry with `inputs: []` to state that the output has no tracked upstream dataset.
3. **Construct the facet**: Wrap all entries in a `LineageJobFacet` on the job.

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

Initially, we disallow mixing facet types. As the model matures and interaction semantics become well-understood, this restriction could be relaxed — for example, allowing `LineageDatasetFacet` on output datasets in a RunEvent alongside `LineageJobFacet` on the job, with clear merge/precedence rules.

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
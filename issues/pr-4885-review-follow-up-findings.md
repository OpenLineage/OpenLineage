# PR #4885 review follow-up: Delta write-plan coverage and residual risks

Last updated: 2026-08-31 (Asia/Tehran)

## Scope and constraints

This is an investigation and implementation follow-up for
[OpenLineage PR #4885](https://github.com/OpenLineage/OpenLineage/pull/4885), currently titled
`spark: keep adaptive events for non-Delta writes`. It records the review blocker, additional
write-plan coverage gaps, compatibility risks, filter interactions, and the validation needed
before the change can safely be approved.

No GitHub comments or replies were posted during this investigation. No OpenLineage specification
files are in scope.

The original PR head inspected here is:

```text
7531fb7745aed3042934baec025890ac63ce46f4
```

The exact-event regression tests were first committed at:

```text
a1399a4cb3d54be66afa6c10d4f8f0d774ac1f42
```

The PR is open with a `CHANGES_REQUESTED` review submitted by `mobuchowski` on 2026-08-31. The
automatic and approved Spark CI jobs for this head are green, but the current tests do not prove
the relevant exactly-once lineage invariant.

## Executive conclusion

The PR fixes a real lineage-loss symptom, but its new predicate is attached to the wrong SQL
execution. In a standard Spark 4 + Delta write, the outer execution has the Delta write-command
root but is not adaptive. Delta's nested executions can have an `AdaptiveSparkPlan`, but their own
optimized roots are generic read/transform nodes rather than the outer write command. The current
filter asks both questions of the same `QueryExecution`, so the two conditions do not normally
co-occur. In that environment the PR stops `AdaptivePlanEventFilter` from suppressing any event.

The reviewer's class-hierarchy observation is independently confirmed: V2 CTAS and RTAS have never
implemented `V2WriteCommand` in Spark 3.1 through 4.1. However, merely adding those roots to the
current same-execution predicate does not fix the deeper mismatch on standard Spark 4: their outer
physical roots are atomic create/replace commands, not adaptive plans. CTAS/RTAS recognition becomes
useful when the filter correlates a nested adaptive execution with its root write execution.

Additional confirmed concerns are:

1. Databricks Delta classes use the unrecognized `com.databricks.sql.transaction.tahoe` namespace.
2. A catalog-only CTAS/RTAS correction would misclassify Parquet/ORC operations routed through the
   session-wide `DeltaCatalog`.
3. Spark exposes the required `rootExecutionId` only on SQL START events and only from Spark 3.4;
   SQL END requires retained execution state, and Spark 3.1-3.3 need a separately validated fallback.
4. `DeltaEventFilter` has a related false-positive axis: its session-wide Delta gate can suppress
   top-level non-Delta `Filter`, `LocalRelation`, and `SerializeFromObject` executions.
5. Existing tests either mock an execution-state combination that is not shown to occur or use
   timing/count assertions that can miss late events. The regular Spark 4.2 job excludes Delta;
   approved Databricks jobs do run, but do not assert the duplicate-count invariant.

The implemented correction therefore uses root-execution correlation, not a longer list of Delta
write roots. For Spark 3.4+, a child is suppressible only when `rootExecutionId` differs from its
own ID and the recorded root optimized plan is a Spark `Command`. That command boundary covers
CTAS/RTAS without depending on their separate V2 interfaces, and it also handles the nested
non-Delta CTAS pair observed under `DeltaCatalog`. The START-time decision is retained through END.
Spark 3.1-3.3 use a narrow, fail-open call-site fallback. The historical Databricks Spark 3.2
reproduction still needs a real runtime regression test before claiming the fallback fully replaces
the old heuristic there.

## Background

### Current defect: issue #4299

[Issue #4299](https://github.com/OpenLineage/OpenLineage/issues/4299) reports that
`AdaptivePlanEventFilter` suppresses legitimate lineage when:

- the Delta extension is installed session-wide;
- Adaptive Query Execution produces an `AdaptiveSparkPlan`; and
- the current operation is not an actual Delta write.

Examples reproduced for PR #4885 include:

- a Delta input written to plain Parquet; and
- a catalog/Hive aggregation ending in `collect()`.

Before the PR, `isDeltaPlan()` checks only whether `spark.sql.extensions` includes
`io.delta.sql.DeltaSparkSessionExtension`. The adaptive filter then rejects every execution whose
physical root name contains `AdaptiveSparkPlan`. A shuffled non-Delta query can therefore lose its
only SQL START/COMPLETE pair and all of its input/output lineage.

### Historical constraint: issue #1828 and PR #1830

The adaptive filter was introduced by
[PR #1830](https://github.com/OpenLineage/OpenLineage/pull/1830) for
[issue #1828](https://github.com/OpenLineage/OpenLineage/issues/1828). The historical Databricks
reproduction emitted two START and two COMPLETE events for one run.

The supplied workload ended with:

```python
final_df.write.mode("overwrite").saveAsTable("openlineage_poc.employee_shift")
```

The artifact attached to
[the reproduction comment](https://github.com/OpenLineage/OpenLineage/issues/1828#issuecomment-1552662009)
contains this optimized logical-plan root:

```text
org.apache.spark.sql.catalyst.plans.logical.ReplaceTableAsSelect
```

It also contains Databricks implementation classes such as:

```text
com.databricks.sql.transaction.tahoe.commands.CreateDeltaTableCommand
com.databricks.sql.transaction.tahoe.catalog.DeltaCatalog$StagedDeltaTableV2
```

Any correction to #4299 must avoid restoring this historical duplicate behavior while also
avoiding the broader lineage loss.

## PR #4885 implementation inspected

At the inspected head, `isDeltaWritePlan()` reads `QueryExecution.optimizedPlan()` and treats the
root as a Delta write in four situations:

1. the root implementation class begins with `org.apache.spark.sql.delta.`;
2. the root is `SaveIntoDataSourceCommand` and its data-source implementation uses that prefix;
3. the root is `V2WriteCommand`, its `table()` is a `DataSourceV2Relation`, and the relation's table
   implementation uses that prefix; or
4. the root is `InsertIntoHadoopFsRelationCommand` and its file-format implementation uses that
   prefix.

Inspection catches `Exception | LinkageError` and returns `false`, retaining the event. This
fail-open direction is appropriate: an inspection compatibility failure may produce duplicate
events, but it must not silently delete the only lineage event.

The root-only boundary is also correct. Recursively searching all plan children would classify a
Delta read feeding a Parquet target as a Delta write and would reproduce the primary bug.

## Finding matrix

| Case | Current classification | Potential result | Assessment |
| --- | --- | --- | --- |
| Outer Delta write command | Delta write, but non-adaptive | Current gate does not fire | Confirmed on standard Spark 4 plan shapes |
| Nested Delta internal execution | Adaptive, but generic non-write root | Current gate does not fire | Confirmed structural mismatch |
| Delta V2 `CreateTableAsSelect` | Non-Delta outer root | Adding it to same-execution gate changes no standard Spark 4 adaptive event | Confirmed taxonomy gap; insufficient fix alone |
| Delta V2 `ReplaceTableAsSelect` | Non-Delta outer root | Same as CTAS; historical Databricks behavior still unverified | Confirmed taxonomy gap; review fact is correct |
| Databricks Delta command root | Non-Delta | Duplicate/internal events may be retained | Confirmed classifier gap; runtime impact conditional |
| Databricks Delta V2 table/provider | Non-Delta | Duplicate/internal events may be retained | Confirmed classifier gap; runtime impact conditional |
| Non-Delta CTAS through configured `DeltaCatalog` | Currently retained | A catalog-only fix would delete valid lineage | High-risk implementation trap |
| Detected Delta write with no real duplicate | Filtered | The only useful lineage event may be deleted | Unresolved architectural risk |
| Nested non-Delta execution | Not semantically distinguished by nesting alone | Blanket nested-event removal could delete valid lineage | Required control for proposed solution |
| Top-level non-Delta `Filter`/`LocalRelation`/`SerializeFromObject` in Delta session | Filtered by `DeltaEventFilter` | Valid read-only lineage can be deleted | Confirmed adjacent false positive |
| Spark `ReplaceData`/`WriteDelta` with `RowLevelOperationTable` | Non-Delta | Native V2 row-level Delta write may be missed | Conditional/future risk |
| Kernel-backed `io.delta.spark.internal.v2` table | Non-Delta | New/opt-in V2 Delta writes may be missed | Forward-looking risk |
| Delta internal adaptive query with a generic root | Retained by adaptive filter | May still be caught by `DeltaEventFilter`; otherwise noise | Conditional; blanket noise claim not confirmed |
| Delta execution with no or wrong active session | Filter disabled | Duplicate protection silently disabled | Existing limitation |
| Delta read followed by Parquet write | Non-Delta | Valid SQL events retained | Correct behavior |
| Read-only aggregation with Delta extension installed | Non-Delta | Valid SQL events retained | Correct behavior |

## Independent verification of the follow-up report

The supplied report combines a strong architectural discovery with several measurements that were
not accompanied by their harness or raw output. This investigation therefore distinguishes the
reproducible facts from the exact reported counts.

### Confirmed: the two halves of the new gate describe different executions

`AdaptivePlanEventFilter` evaluates both of these properties on one `QueryExecution`:

```text
optimized plan is a recognized Delta write root
executed plan root contains AdaptiveSparkPlan
```

Standard Spark 4 planning separates them:

| Optimized root | Physical root | Adaptive | Delta-write root in PR |
| --- | --- | --- | --- |
| `AppendData` | `AppendDataExecV1` | no | yes |
| `OverwriteByExpression` | `OverwriteByExpressionExecV1` | no | yes |
| `SaveIntoDataSourceCommand` | `Execute SaveIntoDataSourceCommand` | no | yes |
| Delta `MergeIntoCommand` | `Execute MergeIntoCommand` | no | yes |
| `CreateTableAsSelect` | `AtomicCreateTableAsSelect` | no | no |
| `ReplaceTableAsSelect` | `AtomicReplaceTableAsSelect` | no | no |
| nested `Project`/`Aggregate`/`SerializeFromObject`/`Repartition` | `AdaptiveSparkPlan` | yes | no |

This is the major new problem found by the follow-up report. The reviewer is right about CTAS/RTAS
inheritance, but extending `isDeltaWritePlan()` with those two outer roots cannot make the current
same-execution conjunction true on these standard Spark 4 shapes.

Preserved local Spark-listener logs independently support this separation. In a Delta batch write,
the user operation had `executionId=1, rootExecutionId=1`; Delta launched executions 2-5 with
`rootExecutionId=1`. A later top-level Parquet operation had `executionId=6,
rootExecutionId=6`, with executions 7-9 rooted at 6. On the PR build, the intended Parquet and
read-only adaptive events were retained. The supplied report's exact totals (`122`, `34`, and `31`)
were not independently reproduced and should not be quoted as repository-verified numbers.

### Confirmed: CTAS/RTAS hierarchy and API break

`javap` inspection of the cached `spark-catalyst` artifacts produced this matrix:

| Spark | CTAS/RTAS create-table interface | `V2WriteCommand`? |
| --- | --- | --- |
| 3.1.3 | `V2CreateTablePlan` | no |
| 3.2.4 | `V2CreateTablePlan` | no |
| 3.3.4 | `V2CreateTablePlan` | no |
| 3.4.4 | `V2CreateTablePlan` | no |
| 3.5.6 | `V2CreateTableAsSelectPlan` | no |
| 4.0.0 | `V2CreateTableAsSelectPlan` | no |
| 4.1.0 | `V2CreateTableAsSelectPlan` | no |

Through Spark 3.4, `V2CreateTablePlan.tableName()` returns an `Identifier`. From Spark 3.5,
`V2CreateTableAsSelectPlan.name()` returns a `LogicalPlan`, with a compatibility `tableName()`
implementation. Shared code compiled against Spark 3.2 cannot directly use the newer interface;
reflection or version-specific code is required. Provider metadata remains the appropriate target
signal because the session catalog can serve non-Delta providers too.

### Confirmed with constraints: `rootExecutionId` is the useful discriminator

Spark 3.4 added `SparkListenerSQLExecutionStart.rootExecutionId`. A top-level execution reports its
own ID; a nested execution reports the outer root's ID. The repository already reads this method
reflectively in
[`OpenLineageSparkListener`](../integration/spark/app/src/main/java/io/openlineage/spark/agent/OpenLineageSparkListener.java)
for job-metrics correlation.

There are two important constraints omitted from the proposed sketch:

- Spark 3.1-3.3 START events have no root-execution field or equivalent execution property.
- SQL END events do not expose `rootExecutionId`, including in Spark 4.1.

The listener must therefore record the START-time root association and reuse it at END. For Spark
3.1-3.3, merely treating every execution started while another execution is active as nested is
unsafe: independent top-level queries can overlap on different threads. The old behavior may be a
compatibility fallback, but a new correlation algorithm for those versions requires concurrency
tests and, especially, the original Databricks Spark 3.2 fixture.

### Qualified or not confirmed

- The original #1828 artifact confirms Databricks `tahoe` classes, an RTAS optimized root, an
  `AtomicReplaceTableAsSelectExec` stack, and duplicated terminal events. It does not prove that
  standard Spark 4 nesting matches the historical Databricks 3.2 cause. Saying CTAS/RTAS handling
  categorically cannot help that linked reproduction goes beyond the evidence.
- The claim that all newly retained `Aggregate` and `Repartition` executions become OpenLineage
  runs is too broad. `DeltaEventFilter` traverses plan leaves for known Delta transaction columns.
  In available local logs, five `Aggregate` roots were rejected by that filter on both baseline and
  PR builds. New internal noise remains possible, but the specific event-count claim is unverified.
- The Mockito fake-package technique is brittle, but current positive assertions would fail if the
  generated mock no longer inherited the expected package. An explicit class-name assertion would
  improve diagnostics; this is not an additional functional bug.
- The report's statement that every Delta assertion on this PR used Delta 1.1 is too broad. The
  approved CI also ran the live Databricks suite on DBR 13.3 and 16.4. Its CTAS test verifies the
  input/output of the last event, however, not an exact terminal-event count or the filter's
  execution pairing, so the green result still cannot catch this bug.

## Detailed findings

### 1. V2 CTAS and RTAS are outside `V2WriteCommand`

The review comment correctly identifies a definite class-hierarchy gap:

> V2WriteCommand does not cover CreateTableAsSelect or ReplaceTableAsSelect; those implement
> Spark's separate V2 create-table interfaces.

Relevant source:

- [review comment](https://github.com/OpenLineage/OpenLineage/pull/4885#discussion_r3893548905)
- [Spark 3.2 V2 commands](https://github.com/apache/spark/blob/v3.2.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/v2Commands.scala)
- [Spark 4.0 V2 commands](https://github.com/apache/spark/blob/v4.0.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/v2Commands.scala)

Spark's hierarchy is effectively:

```text
V2WriteCommand
  - AppendData
  - OverwriteByExpression
  - OverwritePartitionsDynamic
  - ReplaceData              (through RowLevelWrite on newer Spark)
  - WriteDelta               (through RowLevelWrite on newer Spark)

V2CreateTablePlan            (separate hierarchy)
  - CreateTable
  - CreateTableAsSelect
  - ReplaceTable
  - ReplaceTableAsSelect
```

Only the `...AsSelect` operations carry a query that can produce an adaptive plan. Plain
`CreateTable` and `ReplaceTable` do not need adaptive-write classification because they do not
execute a select query.

Affected public operations include:

```sql
CREATE TABLE target USING delta AS SELECT ...
REPLACE TABLE target USING delta AS SELECT ...
CREATE OR REPLACE TABLE target USING delta AS SELECT ...
```

and:

```python
df.write.mode("overwrite").saveAsTable("target")
df.writeTo("target").create()
df.writeTo("target").createOrReplace()
df.writeTo("target").replace()
```

For the historical `ReplaceTableAsSelect` outer root, every existing branch returns false:

```text
Delta-package command root:                 false (Spark Catalyst class)
SaveIntoDataSourceCommand:                  false
V2WriteCommand:                             false
InsertIntoHadoopFsRelationCommand:           false
```

This is a real classifier omission. It is not, by itself, proof that the current adaptive filter
retains a duplicate: on standard Spark 4 the outer `AtomicReplaceTableAsSelect` execution is not
adaptive, while adaptive children have different, generic optimized roots. Adding RTAS to the
same-execution check therefore changes no event in that environment.

RTAS/CTAS detection is still needed if the solution classifies the *root execution* for a nested
adaptive child, and it may be needed for Databricks-specific shapes. The latter must be tested on
the historical runtime rather than inferred from Apache Spark 4.

### 2. Databricks Delta namespaces are not recognized

The PR uses the single prefix:

```text
org.apache.spark.sql.delta.
```

Databricks uses proprietary names under:

```text
com.databricks.sql.transaction.tahoe.commands
com.databricks.sql.transaction.tahoe.catalog
```

This is not speculative naming. The original #1828 artifact contains those classes, and the
OpenLineage repository already has dedicated compatibility code:

- [`UpdateCommandUtils`](../integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/utils/UpdateCommandUtils.java)
- [`DeleteCommandUtils`](../integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/utils/DeleteCommandUtils.java)
- [`AbstractDatabricksHandler`](../integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/lifecycle/plan/catalog/AbstractDatabricksHandler.java)

Consequently, the current detector can miss:

- Databricks `MergeIntoCommand`, `UpdateCommand`, `DeleteCommand`, `CopyIntoCommand`, and related
  command variants;
- V2 writes whose `DataSourceV2Relation.table()` is a Databricks Delta table implementation; and
- Databricks-specific provider, file-format, or staged-table implementations.

There is a second gate: `isDeltaPlan()` still checks only for the exact OSS extension class
`io.delta.sql.DeltaSparkSessionExtension`. On a managed runtime that does not expose that exact
configuration, the adaptive filter is disabled before target inspection. On a runtime that does
expose the OSS extension name but uses proprietary Delta implementation classes, target inspection
returns false.

In either case, the current PR does not establish that the original Databricks duplicate protection
still works.

### 3. CTAS/RTAS must be provider-aware, not catalog-name-aware

A correction must not treat every CTAS/RTAS routed through `DeltaCatalog` as a Delta target.

Delta's catalog extends Spark's `DelegatingCatalogExtension`. It handles a create operation as
Delta only when the target provider is Delta; otherwise it delegates to the underlying catalog:

- [Delta 4.4 `AbstractDeltaCatalog.createTable`](https://github.com/delta-io/delta/blob/v4.4.0/spark/src/main/scala/org/apache/spark/sql/delta/catalog/AbstractDeltaCatalog.scala#L594-L635)

This non-Delta query commonly runs with the same configured catalog:

```sql
CREATE TABLE parquet_target
USING parquet
AS SELECT * FROM delta_source
```

If a CTAS/RTAS fix checks only whether the catalog class is `DeltaCatalog`, it will classify the
Parquet target as Delta and suppress its only adaptive terminal event. That would recreate #4299
for a new write-plan shape.

The correct primary signal is the target provider, resolved case-insensitively as Delta. Catalog
identity can be supplementary evidence but cannot be sufficient by itself.

### 4. CTAS/RTAS metadata access varies across Spark and Databricks

The provider is not exposed through one binary-stable accessor across all supported runtimes:

- Spark 3.2 stores the provider in the CTAS/RTAS properties map using
  `TableCatalog.PROP_PROVIDER`.
- Spark 3.3+ normally exposes it through `tableSpec().provider()`.
- Databricks Catalyst can change method return descriptors relative to the corresponding Apache
  Spark release.

The repository already documents this problem in
[`V2CreateTablePlanUtils`](../integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/utils/V2CreateTablePlanUtils.java).
That utility reads `name`, `catalog`, `tableName`, `tableSpec`, `properties`, and `writeOptions`
reflectively, with fallbacks for Spark-version and Databricks differences.

Direct shared-module calls to `tableSpec()` risk `NoSuchMethodError`. If that error reaches the
PR's fail-open boundary, the listener remains safe from lineage deletion, but the historical
duplicate protection is again disabled.

The shared-module CTAS/RTAS detector therefore needs compatible reflective provider extraction or a
version-specific extension point. It should not add a compile-time dependency from `shared` to the
`spark3` module, because the module dependency direction is the opposite.

Provider matching should also handle:

- case variations of `delta`;
- `TableCatalog.PROP_PROVIDER` in the older properties map;
- `tableSpec.provider` on newer Spark;
- missing or incompatible metadata by retaining the event; and
- preferably the fully qualified Delta provider class if Spark preserves that instead of the short
  name.

### 5. Actual Delta writes may still lose their only useful event

The PR narrows the old heuristic but retains this assumption:

```text
actual Delta write + AdaptiveSparkPlan => the adaptive event is redundant
```

It never proves that a second, useful terminal event was or will be emitted.

In
[issue #4299's follow-up investigation](https://github.com/OpenLineage/OpenLineage/issues/4299#issuecomment-3834992346),
the reporter disabled both adaptive and Delta filters on Spark 4 + Delta 4 + Kyuubi. They observed
32 events across 13 run IDs, representing separate internal Spark jobs, but no duplicate START or
COMPLETE event for the same run ID. They could not reproduce the Databricks `2 START + 2 COMPLETE`
pattern on standard Spark.

This suggests the historical duplicate may be runtime- or version-specific. On standard Spark 4,
the PR's new same-execution conjunction is not shown to fire, so this particular deletion path is
not reached. On another runtime where a recognized Delta write root and adaptive executed root do
co-occur, the filter would still delete the event without first proving a replacement exists.

This is not a regression introduced by #4885; it is behavior intentionally preserved by the PR.
Nevertheless, target classification alone cannot establish correctness. The actual invariant is:

```text
For each real query execution, exactly one useful terminal lineage event survives.
```

A longer-term replacement may use observed event/execution identity rather than plan-type
inference. Simple deduplication by SQL execution ID must first be tested against nested Delta
executions and the original artifact, because internal executions may have distinct execution IDs
while contributing to one user-visible operation.

### 6. Spark generic V2 row-level writes wrap the real table

On Spark 3.5+, generic V2 `DELETE`, `UPDATE`, and `MERGE` operations can be rewritten to
`ReplaceData` or `WriteDelta`. Those extend `V2WriteCommand`, so their class hierarchy appears to be
covered.

However, Spark builds a `DataSourceV2Relation` whose table is a Spark-owned
`RowLevelOperationTable`, not the connector's original table:

```text
ReplaceData or WriteDelta
  -> DataSourceV2Relation
       -> RowLevelOperationTable
            -> original connector table
```

Primary source:

- [Spark `RowLevelOperationTable`](https://github.com/apache/spark/blob/v4.0.0/sql/catalyst/src/main/scala/org/apache/spark/sql/connector/write/RowLevelOperationTable.scala#L30-L50)
- [Spark row-level rewrite](https://github.com/apache/spark/blob/v4.0.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/RewriteRowLevelCommand.scala)

The PR checks only the immediate `relation.table()` implementation class. It therefore sees a
Spark class and returns false even if the wrapped original table is Delta.

Current OSS Delta generally rewrites row-level commands to its own command classes, which the OSS
package-prefix branch catches. This wrapper issue is therefore not a demonstrated blocker for the
default current Delta path, but it is a compatibility risk for native V2 row-level Delta support,
alternate Delta connectors, and future implementations.

The wrapper is Spark-private, so unwrapping it may need reflection and should remain fail-open.

### 7. The newer Kernel-backed Delta V2 implementation uses another package

Delta Lake 4.4 contains a newer V2 table implementation:

```text
io.delta.spark.internal.v2.catalog.DeltaV2Table
```

Source:

- [Delta 4.4 `DeltaV2Table`](https://github.com/delta-io/delta/blob/v4.4.0/spark/v2/src/main/java/io/delta/spark/internal/v2/catalog/DeltaV2Table.java#L91-L94)
- [Delta 4.4 V2 enable-mode documentation](https://github.com/delta-io/delta/blob/v4.4.0/spark/src/main/scala/org/apache/spark/sql/delta/sources/DeltaSQLConf.scala#L3603-L3628)

This does not match the PR's OSS prefix. Delta 4.4 documents the Kernel-backed V2 connector as
non-default and still limited relative to the legacy connector, so it is not a reason by itself to
block this narrow PR. It does demonstrate that the package-prefix design is already incomplete for
the latest Delta source and should either be extended or tracked explicitly as follow-up work.

### 8. Narrowing the adaptive filter may expose Delta internal queries

The old adaptive filter suppressed all adaptive executions whenever the Delta extension was
installed. The PR retains adaptive events unless the current optimized-plan root is an identified
Delta write.

That is necessary for user aggregations and non-Delta writes, but a Delta transaction may launch
internal queries with generic roots such as `Project`, `Aggregate`, or other non-write nodes. The
separate `DeltaEventFilter` suppresses several known internal shapes:

- local relations;
- filter roots;
- Delta transaction-log column signatures;
- Delta log projections;
- `SerializeFromObject`;
- staged Delta tables; and
- job-level START/END events.

Those are targeted patterns rather than a complete semantic marker for all current and future
Delta internal executions. An internal adaptive query that does not match them will now be retained.

Potential impact:

- extra OpenLineage runs for Delta internals;
- higher event volume;
- user-visible noise even when the main lineage is correct.

No concrete new internal-event regression was demonstrated during this review. It should be tested
with exact event counts for complex writes and treated as a compatibility risk rather than a
confirmed blocker.

### 9. `isDeltaPlan()` depends on the global active session

`isDeltaPlan()` obtains configuration through `SparkSessionUtils.activeSession()`, not through the
`SparkSession` carried by the current `OpenLineageContext`.

Relevant helper:

- [`SparkSessionUtils.activeSession`](../integration/spark/shared/src/main/java/io/openlineage/spark/agent/util/SparkSessionUtils.java)

If listener processing occurs asynchronously with no active session, the helper returns empty and
the adaptive filter is disabled. In a multi-session application, the active session could also be
different from the query's session.

Consequences:

- actual Delta query + no/wrong active session -> duplicate suppression silently disabled;
- non-Delta query + unrelated active Delta session -> the new write-target check generally keeps
  the query safe, because the target is still non-Delta.

This is a pre-existing limitation, not a new regression from #4885. It fails open, so its principal
risk is duplicate/noisy events rather than missing lineage. It is related in mechanism, though not
identical in consequence, to the asynchronous session-teardown problem described in
[issue #4888](https://github.com/OpenLineage/OpenLineage/issues/4888).

### 10. `DeltaEventFilter` has the same session-wide false-positive axis

Every root-shape heuristic in `DeltaEventFilter` is gated by the same session-level
`isDeltaPlan()` check. It does not first establish that the current execution is nested under a
Delta write. Consequently, a Delta-enabled session can suppress top-level, read-only non-Delta
queries whose roots happen to match:

- `Filter`;
- `LocalRelation`; or
- `SerializeFromObject`.

The transaction-log column checks are more specific, but the three root-only checks above are not.
This is a confirmed adjacent lineage-loss risk and strengthens the case for execution-context
classification shared by both filters.

Nesting alone must not become the replacement semantic marker. Nested SQL executions also occur
outside Delta, and independent top-level executions can overlap. A safe rule should establish both:

1. that the current execution is a nested child; and
2. that its recorded root execution is an actual Delta write or matches another narrowly defined
   internal-operation signature.

### 11. Fail-open inspection behavior is appropriate

The new detector catches inspection exceptions and linkage errors and keeps the event. This can
allow duplicates if a runtime is incompatible, but the alternative would silently delete the only
lineage event.

This behavior should be preserved in the CTAS/RTAS and wrapper work. Tests should explicitly cover:

- `NoSuchMethodError` from a runtime signature mismatch;
- absent provider metadata;
- inaccessible reflective members; and
- unexpected target wrappers.

Expected result in every case: do not filter the event.

## Cases that are already handled or are not current problems

### Delta read followed by a non-Delta write

This must remain unfiltered. Only the target format matters:

```python
spark.read.format("delta").load(source).write.parquet(target)
```

Recursively finding a Delta input is not evidence of a Delta write.

### Read-only aggregation or `collect()`

There is no write target, so the adaptive SQL event must remain. This is one of the demonstrated
#4299 regressions corrected by the PR.

### Standard OSS V2 append and overwrite

For the legacy OSS Delta table implementation, ordinary `AppendData`,
`OverwriteByExpression`, and `OverwritePartitionsDynamic` normally carry a direct
`DataSourceV2Relation` whose table is under `org.apache.spark.sql.delta`. The PR's classifier
correctly recognizes those outer logical roots. Their command physical roots are not normally
adaptive, so this does not establish that the combined adaptive-filter predicate is effective.

### OSS MERGE, UPDATE, and DELETE command rewrites

Current OSS Delta generally rewrites these operations to Delta command roots such as
`MergeIntoCommand`, `UpdateCommand`, and `DeleteCommand`. The OSS prefix recognizes those outer
logical roots, but again their command physical roots are not normally adaptive. Databricks and
generic/native V2 paths remain separate concerns described above.

### Structured Streaming write roots

Streaming logical nodes such as `WriteToMicroBatchDataSourceV1` are not handled by the detector,
but Structured Streaming uses its incremental execution path rather than the same batch
`AdaptiveSparkPlan` root. No concrete adaptive-filter regression for these streaming roots was
found. They should not be added merely for taxonomy completeness without a real adaptive event
shape.

### `InsertIntoDataSourceDirCommand`

Spark's `INSERT OVERWRITE DIRECTORY USING ...` command accepts only providers implementing
`FileFormat`. Delta's data-source provider is not a plain `FileFormat`, so this is not a normal
supported Delta write path and does not need to be classified here.

## Test coverage assessment

### PR unit tests

The PR adds mock-based coverage for:

- Delta command roots;
- `SaveIntoDataSourceCommand` providers;
- direct V2 `AppendData` targets;
- `InsertIntoHadoopFsRelationCommand` file formats;
- non-Delta controls;
- pure Delta reads;
- Delta-read -> Parquet-write; and
- fail-open linkage errors.

Missing unit coverage includes:

- `CreateTableAsSelect`;
- `ReplaceTableAsSelect`;
- Spark 3.2 provider-in-properties shape;
- Spark 3.3+/4 `tableSpec.provider` shape;
- Databricks signature and class-name shapes;
- non-Delta CTAS/RTAS under `DeltaCatalog`;
- generic V2 row-level wrappers; and
- new `io.delta.spark.internal.v2` table classes.

There are also three test-design weaknesses in the new coverage:

- `AdaptivePlanEventFilterTest` stubs `isDeltaWritePlan(context)` and
  `sparkPlan.nodeName()` independently, so it constructs the combination "Delta write root and
  adaptive executed root" without proving that one real `QueryExecution` can have both.
- `testParquetWriteWithDeltaChildrenIsNotDeltaWrite` stubs `plan.children()`, but
  `isDeltaWriteRoot` intentionally inspects only the root. The assertion is correct; the unused
  setup misleadingly suggests a traversal.
- The fake classes depend on Mockito placing a generated mock in a name compatible with the mocked
  Delta type. A direct class-name assertion would improve failure diagnostics, although the
  existing positive classification assertions already make a silent loss of that behavior
  unlikely.

### Existing Delta integration tests

[`SparkDeltaIntegrationTest.testCTASDelta`](../integration/spark/app/src/test/java/io/openlineage/spark/agent/SparkDeltaIntegrationTest.java)
checks that a matching START and COMPLETE event exist for a Delta CTAS. It does not assert that each
occurs exactly once.

`testFilteringDeltaEvents` runs four operations that are documented as producing two events each,
then asserts only:

```java
matchingRequestCount <= 8
```

That assertion accepts every count from zero through eight. It can pass when:

- COMPLETE events are missing;
- only one event per operation survives;
- asynchronous listener delivery has not finished; or
- all relevant lineage is absent.

The shared
[`MockServerUtils.verifyEvents`](../integration/spark/app/src/test/java/io/openlineage/spark/agent/MockServerUtils.java)
waits until matching bodies are observed, but does not assert that matching events appear exactly
once.

`testNoDuplicateEventsForDelta` improves the intended count to `requests.length == 2`, but it uses
Awaitility and succeeds as soon as one poll sees two requests. A third asynchronous request that
arrives after that successful poll does not fail the test. The test needs a bounded quiet period or
a final count assertion after listener synchronization.

The Delta dependency registry currently provides real Delta artifacts only for Spark 3.2.4,
3.3.4, 3.4.4, 3.5.6, and 4.0.0. The PR's regular Spark integration jobs ran Spark 3.2.4 and 4.2.0;
Spark 4.2.0 has no registry entry, so Delta-tagged tests are excluded there. Green CI therefore
does not validate the reported standard Spark 4 + Delta 4 plan pairing; the real Delta integration
coverage on that pair of regular jobs is Spark 3.2.4 + Delta 1.1.0.

The approved workflow also ran `DatabricksIntegrationTest` on DBR 13.3 and 16.4. That is valuable
runtime coverage and means the supplied report's "every Delta assertion used Delta 1.1" statement
is not literally correct. Its CTAS case obtains the last emitted event and checks its input/output;
it does not assert one START plus one COMPLETE, reject late events, record RTAS plan shapes, or
prove that `AdaptivePlanEventFilter` fired. It therefore does not close the reported gap.

As a result, green Spark integration CI is not sufficient evidence that #4885 preserves one and
only one correct terminal lineage pair.

### Integration coverage added on the follow-up branch

The follow-up test changes replace the first-poll duplicate check with final assertions after the
Spark listener bus is empty. The assertion inspects the complete isolated event stream, rather
than first filtering it to the expected job, and requires:

- exactly `START`, then `COMPLETE`;
- one run ID across both events;
- both events to describe the intended top-level operation; and
- the expected inputs and outputs on `COMPLETE`.

[`SparkDeltaIntegrationTest`](../integration/spark/app/src/test/java/io/openlineage/spark/agent/SparkDeltaIntegrationTest.java)
now enables AQE explicitly and covers:

- Delta path save, append, DataFrame overwrite, CTAS, RTAS, and MERGE;
- a Delta read followed by a Parquet write;
- the issue #4299 read-only `GROUP BY ... LIMIT 1000` shape; and
- Parquet CTAS while `DeltaCatalog` is the session catalog.

The tests were run against both Spark 3.2.4 + Delta 1.1.0 and Spark 4.0.0 + Delta 4.0.0 at
regression-test commit `a1399a4cb`. Those deliberately pre-fix results were mixed:

| Scenario | Spark 3.2.4 | Spark 4.0.0 | Observed failure on Spark 4.0.0 |
|---|---:|---:|---|
| Delta path save | pass | pass | - |
| Delta append | pass | pass | - |
| Delta DataFrame overwrite | pass | **fail** | outer RTAS pair plus nested `OverwriteByExpressionExecV1` pair |
| Delta CTAS with join | pass | pass | - |
| Delta RTAS | **fail** | **fail** | outer RTAS pair, nested `OverwriteByExpressionExecV1` pair, and nested `ColumnarToRow` pair |
| Delta MERGE | pass | pass | - |
| Delta read -> Parquet write | pass | pass | - |
| Read-only aggregation | pass | pass | - |
| Parquet CTAS under `DeltaCatalog` | pass | **fail** | outer CTAS pair plus nested `InsertIntoHadoopFsRelationCommand` pair |

The exact Spark 3.2 RTAS failure is four events: the outer RTAS pair plus a nested
`ColumnarToRow` pair. These failures are the intended regression signal; the tests must remain red
until production filtering preserves the real terminal pair without exposing the internal pair.

The new real-plan
[`AdaptivePlanEventFilterIntegrationTest`](../integration/spark/app/src/test/java/io/openlineage/spark/agent/filters/AdaptivePlanEventFilterIntegrationTest.java)
captures SQL START and END events, the real `QueryExecution`, optimized and executed roots, filter
decisions, and `rootExecutionId` reflectively when Spark provides it. It covers path save, append,
overwrite, CTAS, RTAS, and MERGE without coupling the required result to a particular CTAS/RTAS
classification implementation. On both tested version pairs:

- the CTAS/RTAS shape test passes, proving that their outer executions are non-adaptive while
  adaptive inner executions perform work;
- Parquet and ORC CTAS controls pass under `DeltaCatalog` and remain non-Delta;
- adaptive children not handled by `DeltaEventFilter` fail the protection assertion for all six
  Delta write shapes because `AdaptivePlanEventFilter` rejects none of them; and
- top-level `Filter`, `LocalRelation`, and `SerializeFromObject` queries fail because
  `DeltaEventFilter` rejects all three merely in a Delta-enabled session.

On Spark 4.0, the captured root IDs also prove that the rejected expectation concerns nested
executions: for example, path-save execution 50 owns adaptive execution 51, CTAS execution 67 owns
adaptive execution 70, RTAS execution 78 owns adaptive execution 80, and MERGE execution 87 owns
adaptive executions 91 and 95. The specific IDs vary by run; the parent/child relationship is the
stable assertion.

[`DatabricksIntegrationTest`](../integration/spark/app/src/test/java/io/openlineage/spark/agent/DatabricksIntegrationTest.java)
now isolates CTAS setup events and adds the historical RTAS shape: a joined DataFrame written with
`format("delta").mode("overwrite").saveAsTable(...)`. Both cases require the entire isolated
Databricks event log to contain one terminal pair with the expected DBFS input and output. These
tests compile locally but were not executed because they require a configured Databricks runtime.

This coverage deliberately does not require CTAS/RTAS to implement `V2WriteCommand` or force the
fix to enumerate those roots. A nesting-based implementation can satisfy it. Spark 3.5 and a live
Databricks runtime were still unverified at that pre-fix stage; post-fix evidence is recorded below.

## Implemented resolution and post-fix evidence

The production fix now correlates SQL executions with their root command. It does not classify the
current nested plan as a Delta write, and it does not enumerate CTAS, RTAS, OSS Delta, Tahoe, or
Kernel implementation classes.

The listener performs these steps:

1. On SQL START, read `rootExecutionId` reflectively when Spark provides it.
2. Record whether the execution's optimized root implements Spark's common `Command` interface.
3. Mark a child as suppressible only when its root execution is still recorded and is a command.
4. Copy that START-time decision into every `SparkSQLExecutionContext`, including a context rebuilt
   from SQL END after Spark has evicted the START-time `QueryExecution`.
5. Remove the retained association on SQL END and clear all associations at listener shutdown.

This boundary is intentionally different from both attempted target classifiers:

- The PR's same-execution `isDeltaWritePlan && AdaptiveSparkPlan` condition is unreachable on the
  measured standard Delta shapes.
- A root-target classifier is also too narrow for deduplication. On Spark 4, Parquet CTAS under
  `DeltaCatalog` produced an outer CTAS pair plus a nested
  `InsertIntoHadoopFsRelationCommand` pair. Correctly labeling its target as non-Delta retained four
  events. Command-root correlation retains the outer pair and removes only its owned child pair.

Requiring a command root is narrower than discarding every nested SQL execution. A child of a
non-command root is retained, an unknown root fails open, and an overlapping top-level command is
never inferred to be a child merely because another command is active.

For Spark 3.1-3.3, where `rootExecutionId` does not exist, the implementation deliberately accepts
only a narrow fallback:

- the candidate child must not itself be a command;
- exactly one command may be active;
- the child must have the same non-empty description/details call site as the command or contain a
  known OSS/Databricks/Kernel Delta call-site marker; and
- ambiguous or incompatible cases retain their events.

The remaining legacy limitation is explicit: two independent top-level non-command queries issued
from the same call site while exactly one command is active cannot be distinguished from a child on
Spark 3.1-3.3. Multiple active command parents, command-shaped candidates, different call sites,
completed roots, and missing roots all fail open. A live Databricks Spark 3.2 run remains necessary
to measure this fallback against the original proprietary reproduction.

Two adjacent corrections are included because they use the same evidence:

- the broad `Filter`, `LocalRelation`, and `SerializeFromObject` checks in `DeltaEventFilter` now
  apply only to a recorded command child, so top-level read-only plans survive; and
- Delta-extension detection now reads the `SparkContext` stored in `OpenLineageContext` instead of
  the process-global active `SparkSession`, avoiding listener-time session mismatch or teardown.

`AdaptivePlanEventFilter` no longer requires the child physical root to contain
`AdaptiveSparkPlan`. This is necessary for the real Spark 3.2 RTAS duplicate whose child root is
`ColumnarToRow`. The class name remains for compatibility, but the predicate is now an
execution-correlation predicate.

### Final local validation

The final implementation produced these results:

| Validation | Spark 3.2.4 + Delta 1.1.0 | Spark 3.4.4 + Delta 2.4.0 | Spark 4.0.0 + Delta 4.0.0 |
|---|---:|---:|---:|
| Shared/app unit tests for correlation, filters, context retention, and cleanup | pass | not repeated | pass |
| Real-plan filter integration tests | 4/4 pass | 4/4 pass | 4/4 pass |
| Exact terminal-pair scenarios | 9/9 pass | 9/9 pass | 9/9 pass |

The nine exact terminal-pair scenarios are path save, append, overwrite, CTAS, RTAS, MERGE, Delta
read to Parquet write, read-only aggregation, and Parquet CTAS under `DeltaCatalog`. Every test waits
for the listener bus to drain, inspects the complete isolated event stream, requires exactly
`START, COMPLETE` with one run ID, and validates the expected COMPLETE inputs/outputs.

The real-plan suite additionally proves:

- Delta path save, append, overwrite, CTAS, RTAS, and MERGE launch command-owned child executions
  which the production predicate rejects;
- Delta and non-Delta CTAS/RTAS outer roots remain top-level commands;
- nested Parquet and ORC CTAS executions under `DeltaCatalog` are deduplicated while their outer
  terminal pair is retained;
- top-level `Filter`, `LocalRelation`, and `SerializeFromObject` roots remain unfiltered; and
- on Spark 3.4+, an absent/unknown root or a child of a non-command root fails open.

A supplemental Spark 3.5.6 + Delta 3.3.2 run passed seven of the nine exact scenarios. The two
workloads that did not execute were DataFrame overwrite and RTAS; Delta rejected both before filter
assertions with `Table ... does not support truncate in batch mode`. The corresponding two
real-plan tests stopped at the same unsupported operations. This run is not counted as a green
filter matrix and does not provide evidence against the implementation; it records a separate
workload/runtime compatibility limitation that would need a supported Delta/Spark combination or a
different fixture.

The Databricks CTAS and historical RTAS exact-count tests compile locally but were not executed
because no configured Databricks runtime is available. No claim of live proprietary-runtime
validation is made.

## Required validation matrix

This section preserves the pre-fix review checklist. Items D and E were requirements only for a
target-classification design; command-root correlation removes that production dependency. Their
underlying Spark/Databricks variants are still covered where relevant by the exact-event tests and
the live Databricks acceptance item.

### Minimum blocker tests

#### 0. Real execution-pairing test

Run representative Delta append, overwrite, CTAS, RTAS, and MERGE operations with AQE enabled and
capture, for every SQL execution:

```text
executionId
rootExecutionId (when available)
optimized-plan root
executed-plan root
filter decision for START and END
```

Assert that the production predicate fires for the intended nested executions. A mock that supplies
the write-root and adaptive-root properties independently is not an adequate regression test.

#### A. Historical RTAS reproduction

```python
joined.write.format("delta").mode("overwrite").saveAsTable("target")
```

Requirements:

- optimized root is captured as `ReplaceTableAsSelect`;
- exactly one START and one COMPLETE survive for the user-visible run;
- both events use the expected run identity;
- COMPLETE contains all expected inputs and the Delta output; and
- no late duplicate arrives after the assertion.

Run this on a Databricks runtime if possible. An Apache Spark test double is not sufficient proof
for the proprietary plan and catalog classes in the historical report.

#### B. Delta CTAS with AQE

```sql
CREATE TABLE target USING delta AS
SELECT left.* FROM left JOIN right ON left.id = right.id
```

Assert exactly one START and one COMPLETE with the expected inputs/output.

#### C. Non-Delta CTAS and RTAS under the same `DeltaCatalog`

```sql
CREATE TABLE parquet_target USING parquet AS
SELECT * FROM delta_source
```

and an RTAS/create-or-replace equivalent.

Requirements:

- target is classified as non-Delta despite the configured `DeltaCatalog` and Delta input;
- the adaptive SQL pair survives; and
- COMPLETE contains the Delta input and Parquet output.

#### D. Cross-version provider extraction

Cover:

- Spark 3.2 properties map containing `TableCatalog.PROP_PROVIDER=delta`;
- Spark 3.5+/4 `tableSpec.provider=delta`;
- provider value case variations;
- non-Delta provider controls;
- absent provider metadata; and
- accessor linkage failure retaining the event.

#### E. Databricks class compatibility

At minimum, test class-name recognition for:

```text
com.databricks.sql.transaction.tahoe.commands.*
com.databricks.sql.transaction.tahoe.catalog.*
```

The original RTAS artifact should remain the end-to-end acceptance fixture.

#### F. Root-execution correlation and concurrency

For Spark 3.4+:

- verify top-level `rootExecutionId == executionId` and nested `rootExecutionId != executionId`;
- resolve the root QueryExecution's target, not the nested child's generic plan root;
- assert identical decisions for START and END despite END lacking `rootExecutionId`; and
- clean retained state after success, failure, and listener shutdown.

For every fallback used on Spark 3.1-3.3:

- run two overlapping top-level queries from different threads;
- run a real Delta write with nested SQL executions; and
- prove the fallback does not attach one top-level query to another.

If that cannot be proved, retain a fail-open or explicitly version-limited behavior rather than
silently suppressing events.

#### G. Adjacent `DeltaEventFilter` false positives

With the Delta extension installed, run top-level non-Delta/read-only plans rooted at `Filter`,
`LocalRelation`, and `SerializeFromObject`. Each must keep its useful SQL events. Also retain a real
Delta-internal control so correcting those false positives does not simply disable all internal
filtering.

### Broader regression tests

For each of the following, assert exact event counts and final lineage rather than only event
presence:

- Delta path save;
- append to an existing Delta table;
- overwrite an existing Delta table;
- CTAS;
- RTAS/create-or-replace;
- MERGE;
- UPDATE;
- DELETE;
- Delta-read -> Parquet-write;
- catalog/Hive aggregation ending in `collect()`; and
- a complex Delta write that launches internal queries.

The acceptance invariant should be explicit:

```text
one START + one COMPLETE for the real user-visible execution
expected inputs and outputs on COMPLETE
no duplicate terminal pair
no unexpected user-visible Delta-internal runs
```

### Version matrix

Use at least:

- Spark 3.2.x + Delta 1.x for the older CTAS properties shape;
- Spark 3.5.x + Delta 3.x for current Scala 2.12 behavior;
- Spark 4.0/4.1/4.2 + Delta 4.x for current Scala 2.13 behavior; and
- a supported Databricks runtime for proprietary Catalyst/catalog compatibility.

Mock-only shared-module tests are useful for branches and failure behavior, but the exactly-once
event invariant requires integration evidence.

## Recommended implementation boundary

The implemented boundary is the smallest one that satisfies every locally executable regression:

1. Read Spark's root execution identity at SQL START.
2. Record whether the root optimized plan is a Spark command.
3. Suppress only children owned by that command while the Delta extension is configured or the
   execution is on Databricks.
4. Preserve that decision through SQL END and clear it deterministically.
5. On Spark 3.1-3.3, use only the narrow call-site fallback described above and fail open on
   ambiguity.
6. Apply the same child/root distinction to the three broad `DeltaEventFilter` shapes.
7. Read the Delta configuration from the execution context rather than a global active session.
8. Prove the result with real QueryExecutions, exact terminal counts after listener drain,
   non-Delta command controls, non-command-root controls, and concurrency/cleanup unit tests.

CTAS/RTAS provider extraction, Tahoe package matching, generic row-level table unwrapping, and
Kernel-specific class matching are no longer correctness requirements for this filter because the
implementation does not classify a Delta target. They remain relevant to dataset extraction code,
but adding them here would recreate the wrong abstraction.

The unresolved acceptance item is the live Databricks historical RTAS fixture, especially on the
Spark 3.2-era runtime where the direct root ID is unavailable. The tests are present and compile,
but only an approved runtime can validate that proprietary event order and call-site details match
the legacy fallback.

## Source index

- [PR #4885](https://github.com/OpenLineage/OpenLineage/pull/4885)
- [requested-changes review](https://github.com/OpenLineage/OpenLineage/pull/4885#pullrequestreview-5065363774)
- [inline CTAS/RTAS comment](https://github.com/OpenLineage/OpenLineage/pull/4885#discussion_r3893548905)
- [issue #4299](https://github.com/OpenLineage/OpenLineage/issues/4299)
- [issue #4299 no-duplicate follow-up](https://github.com/OpenLineage/OpenLineage/issues/4299#issuecomment-3834992346)
- [historical issue #1828](https://github.com/OpenLineage/OpenLineage/issues/1828)
- [historical reproduction artifact](https://github.com/OpenLineage/OpenLineage/issues/1828#issuecomment-1552662009)
- [original filter PR #1830](https://github.com/OpenLineage/OpenLineage/pull/1830)
- [Spark 3.2 V2 command source](https://github.com/apache/spark/blob/v3.2.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/v2Commands.scala)
- [Spark 4.0 V2 command source](https://github.com/apache/spark/blob/v4.0.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/v2Commands.scala)
- [Spark 3.3 SQL execution source without root ID](https://github.com/apache/spark/blob/v3.3.4/sql/core/src/main/scala/org/apache/spark/sql/execution/SQLExecution.scala)
- [Spark 3.4 SQL execution source with root ID](https://github.com/apache/spark/blob/v3.4.4/sql/core/src/main/scala/org/apache/spark/sql/execution/SQLExecution.scala)
- [Spark row-level table wrapper](https://github.com/apache/spark/blob/v4.0.0/sql/catalyst/src/main/scala/org/apache/spark/sql/connector/write/RowLevelOperationTable.scala)
- [Delta 4.4 catalog delegation](https://github.com/delta-io/delta/blob/v4.4.0/spark/src/main/scala/org/apache/spark/sql/delta/catalog/AbstractDeltaCatalog.scala)
- [Delta 4.4 Kernel-backed V2 table](https://github.com/delta-io/delta/blob/v4.4.0/spark/v2/src/main/java/io/delta/spark/internal/v2/catalog/DeltaV2Table.java)
- [Delta 4.4 V2 mode configuration](https://github.com/delta-io/delta/blob/v4.4.0/spark/src/main/scala/org/apache/spark/sql/delta/sources/DeltaSQLConf.scala)
- [`V2CreateTablePlanUtils`](../integration/spark/spark3/src/main/java/io/openlineage/spark3/agent/utils/V2CreateTablePlanUtils.java)
- [`OpenLineageSparkListener` root-execution compatibility code](../integration/spark/app/src/main/java/io/openlineage/spark/agent/OpenLineageSparkListener.java)
- [`SparkSqlExecutionNestingTracker`](../integration/spark/shared/src/main/java/io/openlineage/spark/agent/util/SparkSqlExecutionNestingTracker.java)
- [`DeltaEventFilter`](../integration/spark/shared/src/main/java/io/openlineage/spark/agent/filters/DeltaEventFilter.java)
- [Spark integration dependency registry](../integration/spark/app/build.gradle)
- [`SparkDeltaIntegrationTest`](../integration/spark/app/src/test/java/io/openlineage/spark/agent/SparkDeltaIntegrationTest.java)
- [`DatabricksIntegrationTest`](../integration/spark/app/src/test/java/io/openlineage/spark/agent/DatabricksIntegrationTest.java)
- [`MockServerUtils`](../integration/spark/app/src/test/java/io/openlineage/spark/agent/MockServerUtils.java)

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2026 contributors to the OpenLineage project

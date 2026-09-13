---
Author: Mohammad Sadegh
Created: September 13, 2026
Issue: https://github.com/OpenLineage/OpenLineage/issues/4951
---

# Iceberg maintenance report ownership

## Status and scope

This is a reproduction and proposed design, not an implementation of cached-table metrics support.
The injector exception is handled separately in [#4952](https://github.com/OpenLineage/OpenLineage/pull/4952).
The observations below were collected with that safe-skip fix applied, Spark 3.5.6, Scala 2.13,
Iceberg 1.6.0, and Java 17. Iceberg 1.6.0 is the application integration test dependency;
the vendor unit tests use Iceberg 1.7.0 for this Spark version.

The first target is a successful `rewrite_data_files` CALL using bin-pack and the default single
final commit. Sort rewrites, partial-progress commits, direct Java actions, and other maintenance
procedures need separate coverage before support can be claimed. The ownership test also passes
on Spark 3.2.4 / Scala 2.12 / Iceberg 1.4.3 on Java 17.

## Reproduction and observed reports

The existing `SparkIcebergIntegrationTest.testRewriteDataFilesReportsCompactedTable` creates a table,
commits three single-row files separately, and executes:

```sql
CALL spark_catalog.system.rewrite_data_files(
  table => 'default.compaction_target',
  options => map('min-input-files', '2')
)
```

The result reported three rewritten files and one added file. A marker query after the CALL drains
the listener before examining raw OpenLineage events. The uninstrumented baseline produced:

| Operation | Native Iceberg report | OpenLineage result |
| --- | --- | --- |
| CALL plans the original snapshot | `ScanReport`, three result data files | CALL has no input datasets or scan facet |
| Cached-catalog append rewrites the staged tasks | No separate planning or snapshot-commit report | COMPLETE contains the real table as input/output, without Iceberg report facets |
| CALL commits the replacement snapshot | `CommitReport`, operation `replace`, three removed files and one added file | CALL has no output datasets or commit facet |
| Marker query reads the replacement snapshot | A new `ScanReport`, one result data file | The marker append carries that scan report through the existing scan-local path |

This establishes a missing attachment for two reports that Iceberg actually produced. It does not
establish a missing report for the staged append: that append does not own either report.

The integration test now observes the table's existing reporter around the CALL, forwarding every
callback to its original delegate and restoring it in `close()`. This is test-only instrumentation
of Iceberg's private `BaseTable.reporter` field. It is deliberately installed before planning and
is not proposed as a production injection mechanism.

The test verifies that exactly one scan report and one final `replace` report are produced under
the CALL's Spark SQL execution, that their snapshots match the table before and after rewriting,
and that the cached append still identifies the real table without claiming a commit report.
It does not require CALL datasets to remain absent; a future implementation can add them.

Run the scenario from `integration/spark`:

```bash
JAVA_HOME=/path/to/jdk17 ./gradlew :app:integrationTest \
  -Pspark.version=3.5.6 -Pscala.binary.version=2.13 \
  --tests '*SparkIcebergIntegrationTest.testRewriteDataFilesReportsCompactedTable'
```

## Why cached-catalog injection is insufficient

The relevant Iceberg 1.6.0 source contracts are:

- [SparkCachedTableCatalog](https://github.com/apache/iceberg/blob/apache-iceberg-1.6.0/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkCachedTableCatalog.java)
  loads an already-created table from `SparkTableCache`; it does not own an underlying catalog reporter.
- [BaseTable](https://github.com/apache/iceberg/blob/apache-iceberg-1.6.0/core/src/main/java/org/apache/iceberg/BaseTable.java)
  retains its reporter at construction and passes it to newly created scans and snapshot updates.
  Replacing a catalog field later does not update this reference.
- [RewriteDataFilesSparkAction](https://github.com/apache/iceberg/blob/apache-iceberg-1.6.0/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteDataFilesSparkAction.java)
  plans the input files before launching the cached-table rewrites and commits after those rewrites finish.
- [SparkStagedScan](https://github.com/apache/iceberg/blob/apache-iceberg-1.6.0/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkStagedScan.java)
  consumes preplanned tasks and passes a null scan-report supplier to `SparkScan`.
- [SparkWrite.RewriteFiles](https://github.com/apache/iceberg/blob/apache-iceberg-1.6.0/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java)
  stages rewritten files in `FileRewriteCoordinator`. Its `commit()` does not commit the table snapshot.

Consequently, installing a reporter when OpenLineage encounters a cached relation cannot recover
the earlier planning callback. Attaching a later replacement commit to that relation would also
attribute the report to the wrong execution. The SQL listener is asynchronous, so registration
while processing its START event cannot guarantee that it precedes planning.

## Proposed collection boundary

Use a reporter attached before the table is created, through Iceberg's
[public catalog configuration](https://iceberg.apache.org/docs/1.6.0/metrics-reporting/), plus a bridge
from the reporter callback to the enclosing SQL execution. A first implementation could require
explicit catalog reporter configuration; automatic setup would need an earlier synchronous hook.
Do not mutate the private reporter field of an in-use table as the production solution.

The bridge must carry application identity, SQL execution identity, table identity, report type,
and snapshot identity. The instrumented scenario confirms that the planning and final commit
callbacks run with the enclosing CALL's SQL execution ID. This must be verified independently for
other actions, especially partial-progress commits on worker threads. A callback without a proven
execution owner must remain unattached.

Add CALL dataset extraction for the supported procedure, resolving the real table identity rather
than using a cache UUID. On its completed execution, attach the planning report to the input
snapshot and the final replacement report to the output snapshot. Reuse the existing facet
converters. Preserve any configured reporter's callbacks and keep direct scan-local extraction
independent of this bridge.

The current catalog holder's snapshot-only lookup is not sufficient for this bridge. Repeated scans
of one snapshot and concurrent operations must not consume each other's reports. Retain bounded
operation-specific state, consume it on completion, and release it on failure and application
teardown. Collection and attachment must both honor `metricsReporterDisabled=true`.

## Acceptance for the implementation

- The uninstrumented real rewrite emits its two previously missing reports on the CALL, with
  matching table and snapshot identities; the staged append does not claim the final commit.
- Repeated rewrites, overlapping tables, and reads of the same snapshot do not exchange reports or
  accumulate reporter wrappers. Failed operations release their state.
- A configured reporter receives its callbacks exactly once. Disabling metrics or encountering an
  unsupported runtime preserves normal lineage.
- Ordinary catalog reports and direct scan-local reports still work. Additional Spark/Iceberg
  combinations are claimed only after their callback ownership and timing have been exercised.

Until this collection and correlation boundary is implemented and tested, #4951 remains open.

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2026 contributors to the OpenLineage project

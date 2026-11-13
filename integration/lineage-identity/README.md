# Cross-engine dataset identity

Spark, Hive and Flink compile the same Dataset identity normalizer into their agents. It runs
immediately before an OpenLineage `RunEvent` is sent to Gravitino. Gravitino 1.2.1 remains the
required lineage receiver and forwards events to Marquez. All producers must use the same
Gravitino service; they must not also send those events directly to Marquez.

## Configuration

Configure each producer's OpenLineage HTTP transport to point to the Gravitino lineage endpoint:

```yaml
transport:
  type: http
  url: http://gravitino:8090
  endpoint: /api/lineage
```

An HTTP `url` ending in `/api/lineage` is also supported; do not set `endpoint` as well in that
case. The normalizer derives the metadata API base URL from this transport and reuses its headers
and token authentication. The address is deployment configuration, not compiled into the code.
The producer needs permission to list metalakes and catalogs and read catalog and table metadata.
No `dataset-identity-sources.json` or per-table registry is required.

On its first table lookup and every 30 seconds thereafter, the normalizer reads the Gravitino
metalakes and catalogs. It uses catalog connection properties such as `metastore.uris` and
`jdbc-url` to identify physical sources. Catalogs with several endpoint aliases must expose all
aliases in their properties. For an alias not available in a standard property, add the optional
catalog property `openlineage.source.aliases` with comma-separated URIs. This property changes
only when a source endpoint or alias changes, not when tables are created or dropped.

Each table is looked up in the matching catalog. A confirmed table maps to
`metalake / catalog.schema.table`; a confirmed absence maps to a normalized source URI and
`schema.table`. A source that is not in any Gravitino catalog keeps its source URI as the
namespace, so external tables remain distinct. If multiple catalogs claim the same physical
table, the event is rejected until the catalog ownership is unambiguous. Gravitino API failures
also reject the event; they are never treated as proof that a table is external. Storage location
and table name alone do not establish identity.

Spark Gravitino catalog handlers attach a TABLE symlink with a `gravitino://metalake/catalog`
identifier. Hive uses `hive.metastore.uris` when its dataset contains only the `hive` namespace.
Flink SQL extracts the `uri` or `hive.metastore.uris` from its catalog and emits a data source
facet. The old Flink metalake pre-mapping is skipped when its OpenLineage transport points to
`/api/lineage`.

## Job and Run identity

Both `normalizeEvents` and `normalizeJson` preserve the producer's Job namespace and name.
A Job has one stable `(namespace, name)` identity regardless of the namespaces of its input and
output datasets. A Spark Job such as `spark-jobs / daily_orders.aggregate` keeps this identity
whether it accesses one Metalake, several Metalakes or external sources. Each managed Dataset
still uses `metalake / catalog.schema.table`. Different runs of the same logical Job must use
the same Job identity and a different Run ID for each execution.

Configure a stable namespace for each engine deployment, for example:

| Engine | Configuration | Example |
| --- | --- | --- |
| Spark | `spark.openlineage.namespace` | `spark-jobs` |
| Hive | `hive.openlineage.namespace` | `hive-jobs` |
| Flink SQL agent | `openlineage.job.namespace` | `flink-jobs` |
| Flink 1.x DataStream listener | `execution.job-listener.openlineage.namespace` | `flink-jobs` |

Include cluster, tenant or business domain information when needed to distinguish deployments.
The normalizer preserves an existing `default` namespace; deployments must configure their own
stable namespace to avoid collisions between unrelated jobs. Job names must also identify the
logical work consistently across executions; dataset discovery does not assign Job names.

Each normalized event is returned immediately, including START and RUNNING events without
datasets. No events wait for a Dataset namespace, and no per-run Job namespace state is kept.
Job facets, Run IDs, event time and event type are preserved. ParentRunFacet parent and root
identities remain exactly as supplied by the producer or scheduler, including when the parent is
emitted by another process. Scheduler integrations must supply the actual parent and root Run IDs.
This module does not infer scheduler relationships or rewrite scheduler-assigned Job identities
into engine namespaces without an authoritative mapping.

## Scope and limitations

Both top-level datasets and
`columnLineage.dataset` / `columnLineage.fields[*].inputFields` references are normalized.
Non-table datasets such as Kafka topics and file paths retain their identities. The emitter logs
normalization failures and does not provide a durable retry or quarantine queue. This module
does not deduplicate HTTP retries or distinguish DROP followed by a same-name CREATE.

The Dataset namespace identifies ownership; it does not describe the engine's access path.
For example, native Spark tables in a managed HMS resolve to the same metalake and catalog as
tables accessed through the Gravitino Spark connector. The data source facet retains the metadata
service URI. A catalog without a physical metadata endpoint uses its Gravitino catalog API URL;
register a REST service with `openlineage.source.aliases` when that service supplies the metadata.
Storage URI TABLE symlinks alone do not establish managed ownership. For local file datasets,
file URI TABLE symlinks are removed so Marquez exposes only the physical `file` identity rather
than an additional warehouse namespace. Column references and physical paths are preserved.
Other link types and authoritative metastore links retain their existing handling.

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2026 contributors to the OpenLineage project

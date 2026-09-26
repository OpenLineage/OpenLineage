---
sidebar_position: 6
title: Feast
---

[Feast](https://feast.dev/) is an open-source feature store for machine learning. It provides a centralized platform for storing, managing, and serving ML features, enabling teams to define, discover, and use features across training and inference workloads.

To learn more about Feast, visit the [documentation site](https://docs.feast.dev/) or explore the [GitHub repository](https://github.com/feast-dev/feast).

## How does Feast work with OpenLineage?

Feast includes native OpenLineage support as both a **producer** and a **consumer**:

- **Producer**: Feast automatically emits lineage events during feature store operations (`feast apply`, `feast materialize`). No additional code changes required.
- **Consumer**: Feast can receive and store OpenLineage events from any compatible producer (Airflow, Spark, dbt, Flink, etc.), providing a unified lineage view in the Feast UI without requiring a separate metadata platform like Marquez.

The integration uses Feast's existing Python client and FastAPI-based registry server for both producing and consuming events.

## Producer: Emitting Lineage Events

### Registry Changes (`feast apply`)

When you apply feature definitions, Feast creates lineage events that capture:

- **Data Sources → Feature Views**: How raw data flows into feature definitions
- **Entities → Feature Views**: Which entities are associated with features
- **Feature Views → Feature Services**: How features are composed into services

This creates a lineage graph matching Feast's internal model:

```
DataSources ──┐
              ├──→ feast_feature_views_{project} ──→ FeatureViews
Entities ─────┘                                          │
                                                         │
                                                         ▼
                                     feature_service_{name} ──→ FeatureService
```

### Feature Materialization (`feast materialize`)

When materializing features to the online store, Feast emits:

- `START` events when materialization begins
- `COMPLETE` events with row counts on success
- `FAIL` events with error information on failure

## Consumer: Receiving Lineage Events

Feast can act as an OpenLineage consumer, receiving lineage events from any OpenLineage-compatible producer and displaying them in the Feast UI. This enables cross-system data lineage visualization alongside your feature store without deploying a separate metadata platform.

### Consumer Architecture

```
Producers (Airflow, Spark, dbt, Feast, Flink, …)
            │
            ▼
   POST /api/v1/lineage  ──→  Event Processor ──→  Lineage Store (SQL)
                                                          │
                                                          ▼
                                                    Feast UI
                                          ┌──────────────────────────┐
                                          │  Lineage tab             │
                                          │  ├─ OpenLineage Graph    │
                                          │  │   (all producers)     │
                                          │  └─ ☐ Feast Only Lineage │
                                          │      (registry view)     │
                                          │                          │
                                          │  Events tab              │
                                          │  └─ Event browser        │
                                          └──────────────────────────┘
```

### Supported Capabilities

The consumer supports:

- **Standard OpenLineage API** (`POST /api/v1/lineage`) — compatible with all OpenLineage producers
- **Batch ingestion** (`POST /api/v1/lineage/batch`) — for bulk event processing
- **Cross-producer lineage** — automatically links datasets across producers via shared names, `SymlinksDatasetFacet`, or `dataSource` URI matching
- **Run lifecycle tracking** — tracks `START`, `RUNNING`, `COMPLETE`, `FAIL`, and `ABORT` states per run
- **Lineage graph queries** — upstream/downstream traversal from any node
- **RBAC integration** — namespace-based filtering maps OpenLineage namespaces to Feast projects

### Consumer API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/lineage` | `POST` | Receive OpenLineage events (single or array) |
| `/api/v1/lineage/batch` | `POST` | Receive a batch of events |
| `/lineage/openlineage/graph` | `GET` | Full lineage graph with nodes, edges, and symlinks |
| `/lineage/openlineage/graph/{node_type}/{namespace}/{name}` | `GET` | Lineage graph centered on a specific node |
| `/lineage/openlineage/events` | `GET` | Browse stored events with filtering |
| `/lineage/openlineage/jobs` | `GET` | List all OpenLineage jobs |
| `/lineage/openlineage/datasets` | `GET` | List all OpenLineage datasets |
| `/lineage/openlineage/runs` | `GET` | List runs with optional job filtering |
| `/lineage/openlineage/runs/{run_id}` | `GET` | Single run detail with input/output datasets |
| `/lineage/openlineage/reset` | `DELETE` | Purge OpenLineage data (requires API key) |

### Cross-Producer Lineage Connectivity

The consumer automatically links datasets across different producers when they refer to the same physical data:

1. **Shared namespace + name** — If Airflow writes to `s3://bucket/path` and Spark reads from the same dataset, the graph connects them automatically.
2. **SymlinksDatasetFacet** — Producers can declare aliases for their datasets.
3. **dataSource URI matching** — Datasets with matching `dataSource.uri` facets are linked even if their namespace or name differ.

### Database Schema

The consumer creates the following tables automatically:

| Table | Purpose |
|-------|---------|
| `openlineage_events` | Raw event storage with JSON payloads |
| `openlineage_jobs` | Deduplicated job records with producer, description, and facets |
| `openlineage_datasets` | Deduplicated dataset records with schema, facets, and Feast mapping |
| `openlineage_runs` | Run lifecycle tracking (START/COMPLETE/FAIL) |
| `openlineage_run_io` | Input/output relationships between runs and datasets |
| `openlineage_lineage_edges` | Materialized lineage graph edges for traversal |
| `openlineage_dataset_symlinks` | Cross-producer dataset linking |

## Getting Started

### Installation

Install Feast with OpenLineage support:

```bash
pip install feast[openlineage]
```

### Producer Configuration

Add the `openlineage` section to your `feature_store.yaml`:

```yaml
project: my_project
registry: data/registry.db
provider: local
online_store:
  type: sqlite
  path: data/online_store.db

openlineage:
  enabled: true
  transport_type: http
  transport_url: http://localhost:5000
  namespace: feast
```

### Consumer Configuration

Enable the consumer under the `openlineage` section:

```yaml
project: my_project
registry:
  registry_type: sql
  path: postgresql://user:pass@host:5432/feast

openlineage:
  enabled: true
  namespace: my_project
  consumer:
    enabled: true
    store_type: sql
    api_key: "your-api-key"
    namespace_mapping:
      airflow_ns: my_project
      spark_ns: my_project
```

Or via environment variables:

```bash
export FEAST_OPENLINEAGE_CONSUMER_ENABLED=true
export FEAST_OPENLINEAGE_CONSUMER_STORE_TYPE=sql
export FEAST_OPENLINEAGE_CONSUMER_API_KEY=your-api-key
```

### Configuration Options

#### Producer Options

| Option | Description | Default |
|--------|-------------|---------|
| `enabled` | Enable/disable OpenLineage integration | `false` |
| `transport_type` | Transport type (`http`, `kafka`, `console`, `file`) | `http` |
| `transport_url` | URL for HTTP transport | Required for HTTP |
| `namespace` | OpenLineage namespace | `feast` |
| `producer` | Producer identifier | `feast` |
| `emit_on_apply` | Emit events on `feast apply` | `true` |
| `emit_on_materialize` | Emit events on `feast materialize` | `true` |

#### Consumer Options

| Option | Description | Default |
|--------|-------------|---------|
| `consumer.enabled` | Enable the OpenLineage consumer | `false` |
| `consumer.store_type` | Storage backend (`sql`) | `sql` |
| `consumer.connection_string` | Separate DB for lineage (optional, reuses registry DB if omitted) | - |
| `consumer.api_key` | API key producers must provide when sending events | - |
| `consumer.namespace_mapping` | Maps OpenLineage namespaces to Feast projects for RBAC | `{}` |

### Running Feast with OpenLineage

```python
from feast import FeatureStore

fs = FeatureStore(repo_path="feature_repo")

# This automatically emits OpenLineage events (producer)
fs.apply([entity, data_source, feature_view, feature_service])

# This emits START/COMPLETE/FAIL events (producer)
fs.materialize(start_date, end_date)
```

### Configuring External Producers to Send Events to Feast

When the consumer is enabled, configure any OpenLineage producer to send events to your Feast instance:

#### Airflow

```python
OPENLINEAGE_URL = "http://feast-registry:8080/api"
OPENLINEAGE_API_KEY = "your-api-key"
```

#### Spark

```properties
spark.openlineage.transport.type=http
spark.openlineage.transport.url=http://feast-registry:8080/api
spark.openlineage.transport.endpoint=/v1/lineage
spark.openlineage.transport.auth.type=api_key
spark.openlineage.transport.auth.apiKey=your-api-key
```

#### dbt

```yaml
OPENLINEAGE_URL: "http://feast-registry:8080/api"
OPENLINEAGE_API_KEY: "your-api-key"
```

## Custom Facets

Feast emits custom facets to capture feature store-specific metadata:

### FeastFeatureViewFacet

Attached to Feature View datasets:

```json
{
  "feast_featureView": {
    "name": "driver_hourly_stats",
    "ttl_seconds": 86400,
    "entities": ["driver_id"],
    "features": ["conv_rate", "acc_rate", "avg_daily_trips"],
    "online_enabled": true,
    "description": "Hourly driver statistics",
    "owner": "ml-team@example.com",
    "tags": {"team": "ml", "priority": "high"}
  }
}
```

### FeastFeatureServiceFacet

Attached to Feature Service jobs:

```json
{
  "feast_featureService": {
    "name": "driver_activity_service",
    "feature_views": ["driver_hourly_stats", "driver_daily_stats"],
    "feature_count": 6,
    "description": "Driver activity features for prediction",
    "owner": "ml-team@example.com"
  }
}
```

### FeastMaterializationFacet

Attached to materialization run events:

```json
{
  "feast_materialization": {
    "feature_views": ["driver_hourly_stats"],
    "start_date": "2024-01-01T00:00:00Z",
    "end_date": "2024-01-02T00:00:00Z",
    "project": "driver_project"
  }
}
```

## Feast to OpenLineage Mapping

| Feast Concept | OpenLineage Concept |
|---------------|---------------------|
| DataSource | InputDataset |
| FeatureView | OutputDataset (of feature views job) / InputDataset (of feature service job) |
| Feature | Schema field |
| Entity | InputDataset |
| FeatureService | OutputDataset |
| Materialization | RunEvent (START/COMPLETE/FAIL) |

## Where can I learn more?

- [Feast OpenLineage Documentation](https://docs.feast.dev/reference/openlineage)
- [Complete Working Example](https://github.com/feast-dev/feast/tree/master/examples/openlineage-integration)
- [Feast Blog: Tracking Feature Lineage with OpenLineage](https://feast.dev/blog/feast-openlineage-integration)

## Feedback

What did you think of this guide? You can reach out to us on [Feast Slack](https://slack.feast.dev) and leave us feedback!

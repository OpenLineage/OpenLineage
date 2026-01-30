---
sidebar_position: 9
title: Feast
---

[Feast](https://feast.dev/) is an open-source feature store for machine learning. It provides a centralized platform for storing, managing, and serving ML features, enabling teams to define, discover, and use features across training and inference workloads.

To learn more about Feast, visit the [documentation site](https://docs.feast.dev/) or explore the [GitHub repository](https://github.com/feast-dev/feast).

## How does Feast work with OpenLineage?

Feast includes native OpenLineage support that automatically emits lineage events during feature store operations. When you run `feast apply` to register feature definitions or `feast materialize` to populate the online store, Feast captures and emits comprehensive lineage metadata—no additional code changes required.

The integration uses Feast's existing Python client to emit OpenLineage events via HTTP, Kafka, or other supported transports.

## Lineage Events

Feast emits OpenLineage events for the following operations:

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

## Getting Started

### Installation

Install Feast with OpenLineage support:

```bash
pip install feast[openlineage]
```

### Configuration

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

### Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `enabled` | Enable/disable OpenLineage integration | `false` |
| `transport_type` | Transport type (`http`, `kafka`, `console`, `file`) | `http` |
| `transport_url` | URL for HTTP transport | Required for HTTP |
| `namespace` | OpenLineage namespace | `feast` |
| `producer` | Producer identifier | `feast` |
| `emit_on_apply` | Emit events on `feast apply` | `true` |
| `emit_on_materialize` | Emit events on `feast materialize` | `true` |

### Running Feast with OpenLineage

```python
from feast import FeatureStore

fs = FeatureStore(repo_path="feature_repo")

# This automatically emits OpenLineage events
fs.apply([entity, data_source, feature_view, feature_service])

# This emits START/COMPLETE/FAIL events
fs.materialize(start_date, end_date)
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

## Where can I learn more?

- [Feast OpenLineage Documentation](https://docs.feast.dev/reference/openlineage)
- [Complete Working Example](https://github.com/feast-dev/feast/tree/master/examples/openlineage-integration)
- [Feast Blog: Tracking Feature Lineage with OpenLineage](https://feast.dev/blog/feast-openlineage-integration)

## Feedback

What did you think of this guide? You can reach out to us on [Feast Slack](https://slack.feast.dev) and leave us feedback!

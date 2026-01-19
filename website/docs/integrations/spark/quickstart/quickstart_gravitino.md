---
sidebar_position: 4
title: Quickstart with Apache Gravitino
---

Apache Gravitino is an open-source data catalog that provides unified metadata management for various data sources and storage systems. Users of Gravitino can work with data assets such as tables (Iceberg, Hive, etc.) and filesets (storing raw files, on s3, gcs, azure blob, etc).

This guide shows how to integrate OpenLineage with Apache Gravitino to collect data lineage from Spark applications using Gravitino catalogs.

## Prerequisites

- Apache Spark 3.x
- Apache Gravitino server running
- OpenLineage Spark integration jar
- Gravitino Spark connector

## Configuration

To enable OpenLineage with Gravitino integration, you need to configure both the OpenLineage Spark listener and the Gravitino connection parameters.

### Basic Spark Configuration

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .master('local[*]')
    .appName('gravitino_lineage_demo')
    # OpenLineage configuration
    .config('spark.extraListeners', 'io.openlineage.spark.agent.OpenLineageSparkListener')
    .config('spark.jars.packages', 'io.openlineage:openlineage-spark:{{PREPROCESSOR:OPENLINEAGE_VERSION}}')
    .config('spark.openlineage.transport.type', 'console')
    # Gravitino configuration
    .config('spark.sql.gravitino.uri', 'http://localhost:8090')
    .config('spark.sql.gravitino.metalake', 'your_metalake_name')
    # Optional: Gravitino filesystem configuration
    .config('spark.hadoop.fs.gravitino.client.metalake', 'your_metalake_name')
    .getOrCreate())
```

### Configuration Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `spark.sql.gravitino.uri` | Gravitino server URI | `http://localhost:8090` |
| `spark.sql.gravitino.metalake` | Gravitino metalake name | `my_metalake` |
| `spark.hadoop.fs.gravitino.client.metalake` | Metalake for GVFS filesystem | `my_metalake` |

### GVFS Path Format

GVFS (Gravitino Virtual File System) paths follow this format:
```
gvfs://fileset/catalog/schema/fileset[/subpath]
```

Examples:
- `gvfs://fileset/data_catalog/raw_data/user_events/`
- `gvfs://fileset/analytics_catalog/processed/sales_summary/year=2024/month=01/`
- `gvfs://fileset/ml_catalog/features/customer_profiles/latest/features.parquet`

## Example Usage

### 1. Create Spark Session with Gravitino

```python
from pyspark.sql import SparkSession

# Initialize Spark with Gravitino and OpenLineage
spark = (SparkSession.builder
    .master('local[*]')
    .appName('gravitino_lineage_example')
    .config('spark.extraListeners', 'io.openlineage.spark.agent.OpenLineageSparkListener')
    .config('spark.jars.packages', 'io.openlineage:openlineage-spark:{{PREPROCESSOR:OPENLINEAGE_VERSION}}')
    .config('spark.openlineage.transport.type', 'console')
    .config('spark.sql.gravitino.uri', 'http://localhost:8090')
    .config('spark.sql.gravitino.metalake', 'demo_metalake')
    .getOrCreate())

# Set logging level to see OpenLineage events
spark.sparkContext.setLogLevel("INFO")
```

### 2. ETL Example: Hive to MySQL

```python
# Read data from a Hive table managed by Gravitino
spark.sql("USE catalog_iceberg")
spark.sql("CREATE DATABASE IF NOT EXISTS db")
spark.sql("DROP TABLE IF EXISTS catalog_iceberg.db.student")
spark.sql("CREATE TABLE catalog_iceberg.db.student AS SELECT * from parquet.`gvfs://fileset/catalog_fileset/schema/fileset/student/`")

spark.sql("DROP TABLE IF EXISTS catalog_mysql.db.detail")
spark.sql("create table catalog_mysql.db.detail as select student_name, year, avg(score) avg_score from catalog_iceberg.db.detail group by student_name, year")


```

### 3. Working with GVFS (Gravitino Virtual File System)

```python
# Read from a GVFS fileset
df = spark.read.parquet("gvfs://fileset/data_catalog/raw_data/user_events/year=2024/month=01/")

# Process the data
processed_df = df.filter(df.event_type == "purchase") \
    .select("user_id", "product_id", "timestamp", "amount") \
    .withColumn("date", df.timestamp.cast("date"))

# Save to another GVFS fileset with partitioning
processed_df.write \
    .mode("overwrite") \
    .partitionBy("date") \
    .parquet("gvfs://fileset/data_catalog/processed_data/purchase_events/")

# You can also save DataFrames to GVFS paths
sample_df = spark.createDataFrame([
    {'user_id': 1001, 'event': 'login', 'timestamp': '2024-01-15 10:30:00'},
    {'user_id': 1002, 'event': 'purchase', 'timestamp': '2024-01-15 11:45:00'},
    {'user_id': 1003, 'event': 'logout', 'timestamp': '2024-01-15 12:15:00'}
])

# Save to GVFS with automatic schema detection
sample_df.write \
    .mode("append") \
    .option("path", "gvfs://fileset/analytics_catalog/events_schema/user_activity/daily/2024-01-15/") 
```

### 4. Expected OpenLineage Output

When you run ETL operations on Gravitino-managed tables and GVFS paths, OpenLineage will generate events with detailed lineage information:

```json
{
  "eventType": "COMPLETE",
  "eventTime": "2024-12-25T10:00:00.000Z",
  "run": {
    "runId": "550e8400-e29b-41d4-a716-446655440000",
    "facets": {
      "gravitino": {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/{{PREPROCESSOR:OPENLINEAGE_VERSION}}/integration/spark",
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/GravitinoDatasetFacet.json#/$defs/GravitinoDatasetFacet",
        "metalake": "demo_metalake",
        "uri": "http://localhost:8090"
      }
    }
  },
  "job": {
    "namespace": "default",
    "name": "gravitino_lineage_example.insert_into_table"
  },
  "inputs": [{
    "namespace": "http://localhost:8090/api/metalakes/demo_metalake",
    "name": "hive_catalog.sales_db.raw_orders",
    "facets": {
      "schema": {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/{{PREPROCESSOR:OPENLINEAGE_VERSION}}/integration/spark",
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
        "fields": [
          {"name": "customer_id", "type": "long"},
          {"name": "order_date", "type": "date"},
          {"name": "product_name", "type": "string"},
          {"name": "quantity", "type": "integer"},
          {"name": "unit_price", "type": "decimal(10,2)"}
        ]
      }
    }
  }],
  "outputs": [{
    "namespace": "http://localhost:8090/api/metalakes/demo_metalake",
    "name": "mysql_catalog.analytics_db.customer_product_summary",
    "facets": {
      "schema": {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/{{PREPROCESSOR:OPENLINEAGE_VERSION}}/integration/spark",
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
        "fields": [
          {"name": "customer_id", "type": "long"},
          {"name": "product_name", "type": "string"},
          {"name": "total_quantity", "type": "long"},
          {"name": "total_revenue", "type": "decimal(20,2)"}
        ]
      },
      "lifecycleStateChange": {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/{{PREPROCESSOR:OPENLINEAGE_VERSION}}/integration/spark",
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/LifecycleStateChangeDatasetFacet.json#/$defs/LifecycleStateChangeDatasetFacet",
        "lifecycleStateChange": "OVERWRITE"
      }
    }
  }]
}
```

For GVFS operations, you'll see events like:

```json
{
  "eventType": "START",
  "outputs": [{
    "namespace": "http://localhost:8090/api/metalakes/demo_metalake",
    "name": "gvfs://fileset/data_catalog/processed_data/purchase_events/",
    "facets": {
      "dataSource": {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/{{PREPROCESSOR:OPENLINEAGE_VERSION}}/integration/spark",
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
        "name": "gvfs",
        "uri": "gvfs://fileset"
      }
    }
  }]
}
```

## Key Features

### Gravitino Namespace Integration

OpenLineage automatically detects Gravitino catalogs and generates dataset identifiers using the Gravitino namespace format:
- **Namespace**: `{gravitino_uri}/api/metalakes/{metalake_name}`
- **Dataset Name**: `{catalog}.{schema}.{table}` for tables
- **GVFS Path**: `/{catalog}/{schema}/{fileset}[/subpath]` for filesets

### Automatic Configuration Detection

The integration automatically detects Gravitino configuration from Spark session properties:
1. Checks `spark.sql.gravitino.metalake` for connector configuration
2. Falls back to `spark.hadoop.fs.gravitino.client.metalake` for filesystem configuration
3. Uses `spark.sql.gravitino.uri` for the Gravitino server URI

### Multi-Catalog and GVFS Support

OpenLineage supports:
- Multiple Gravitino catalogs within the same Spark session
- GVFS (Gravitino Virtual File System) paths for file-based operations
- Cross-catalog ETL operations with complete lineage tracking

## Troubleshooting

### Common Issues

1. **Missing Gravitino Configuration**
   ```
   Error: Gravitino namespace is required
   ```
   **Solution**: Ensure both `spark.sql.gravitino.uri` and `spark.sql.gravitino.metalake` are configured.

2. **No OpenLineage Events Generated**
   - Verify the OpenLineage Spark listener is properly configured
   - Check that the OpenLineage jar is in the classpath
   - Ensure logging level allows INFO messages

3. **Incorrect Dataset Namespaces**
   - Verify the Gravitino URI is accessible
   - Check that the metalake name matches your Gravitino setup

### Debug Configuration

To debug Gravitino integration, enable debug logging:

```python
spark.sparkContext.setLogLevel("DEBUG")
```

This will show detailed logs about Gravitino configuration detection and dataset identifier generation.

## Next Steps

- Learn more about [OpenLineage Spark Configuration](../configuration/usage)
- Explore [Column-Level Lineage](../spark_column_lineage) with Gravitino
- Check out the [Apache Gravitino documentation](https://gravitino.apache.org/) for advanced catalog management

# Gravitino Integration - Quick Start Guide

Get started with OpenLineage and Apache Gravitino in 5 minutes.

## Step 1: Add Dependencies

Add the required JARs to your Spark application:

```bash
# OpenLineage Spark integration
spark-submit \
  --packages io.openlineage:openlineage-spark_2.12:1.30.0 \
  --jars gravitino-spark-connector-runtime-0.8.0.jar \
  your-application.jar
```

## Step 2: Configure Spark

Add these configurations to your `spark-defaults.conf` or Spark session:

```properties
# Gravitino Configuration
spark.sql.gravitino.metalake=my_metalake
spark.sql.gravitino.uri=http://gravitino-server:8090

# Gravitino Catalog
spark.sql.catalog.gravitino_catalog=org.apache.gravitino.spark.connector.catalog.GravitinoCatalog
spark.sql.catalog.gravitino_catalog.gravitino.metalake=my_metalake
spark.sql.catalog.gravitino_catalog.gravitino.catalog=iceberg_catalog

# OpenLineage Configuration
spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener
spark.openlineage.transport.type=http
spark.openlineage.transport.url=http://openlineage-backend:5000
```

## Step 3: Run Your Spark Job

```scala
val spark = SparkSession.builder()
  .appName("GravitinoQuickStart")
  .getOrCreate()

// Create a table
spark.sql("""
  CREATE TABLE gravitino_catalog.db.users (
    id INT,
    name STRING,
    email STRING
  ) USING iceberg
""")

// Insert data
spark.sql("""
  INSERT INTO gravitino_catalog.db.users 
  VALUES (1, 'Alice', 'alice@example.com')
""")

// Query data
val df = spark.sql("SELECT * FROM gravitino_catalog.db.users")
df.show()
```

## Step 4: Verify Lineage

Check your OpenLineage backend for lineage events:

```bash
curl http://openlineage-backend:5000/api/v1/lineage
```

Expected output structure:
```json
{
  "namespace": "my_metalake",
  "name": "iceberg_catalog.db.users",
  "facets": {
    "schema": {
      "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "email", "type": "string"}
      ]
    }
  }
}
```

## Common Configuration Patterns

### Pattern 1: Multiple Gravitino Catalogs

```properties
spark.sql.gravitino.metalake=production

# Iceberg catalog
spark.sql.catalog.iceberg_prod=org.apache.gravitino.spark.connector.catalog.GravitinoCatalog
spark.sql.catalog.iceberg_prod.gravitino.catalog=iceberg

# JDBC catalog
spark.sql.catalog.postgres_prod=org.apache.gravitino.spark.connector.catalog.GravitinoCatalog
spark.sql.catalog.postgres_prod.gravitino.catalog=postgres
```

### Pattern 2: GVFS Fileset

```properties
spark.sql.gravitino.metalake=production
spark.hadoop.fs.gravitino.client.metalake=production
spark.hadoop.fs.gravitino.client.uri=http://gravitino-server:8090
spark.hadoop.fs.gvfs.impl=org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem
```

```scala
// Read from GVFS
val df = spark.read.parquet("gvfs://fileset/catalog/schema/my_fileset/")
```

### Pattern 3: Catalog Name Mapping

```properties
spark.sql.gravitino.metalake=production
spark.sql.gravitino.catalogMappings=dev_iceberg:prod_iceberg,dev_postgres:prod_postgres
```

## Troubleshooting

### Issue: No lineage events

**Check:**
1. OpenLineage listener is registered: `spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener`
2. Transport URL is correct: `spark.openlineage.transport.url=http://openlineage-backend:5000`
3. Gravitino metalake is configured: `spark.sql.gravitino.metalake=my_metalake`

### Issue: "Gravitino metalake configuration not found"

**Fix:** Add metalake configuration:
```properties
spark.sql.gravitino.metalake=your_metalake_name
```

### Issue: Catalog not found

**Check:**
1. Gravitino server is running and accessible
2. Catalog exists in Gravitino: `curl http://gravitino-server:8090/api/metalakes/my_metalake/catalogs`
3. Catalog configuration matches Gravitino catalog name

## Next Steps

- Read the [full Gravitino integration guide](GRAVITINO.md)
- Explore [OpenLineage Spark configuration options](https://openlineage.io/docs/integrations/spark/configuration/usage)
- Learn about [column-level lineage](https://openlineage.io/docs/integrations/spark/spark_column_lineage)

----
SPDX-License-Identifier: Apache-2.0  
Copyright 2018-2025 contributors to the OpenLineage project

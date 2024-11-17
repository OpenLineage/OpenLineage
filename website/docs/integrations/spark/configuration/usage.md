---
sidebar_position: 1
title: Usage
---


import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Configuring the OpenLineage Spark integration is straightforward. It uses built-in Spark configuration mechanisms. However, for **Databricks users**, special considerations are required to ensure compatibility and avoid breaking the Spark UI after a cluster shutdown.

Your options are:

1. [Setting the properties directly in your application](#setting-the-properties-directly-in-your-application).
2. [Using `--conf` options with the CLI](#using---conf-options-with-the-cli).
3. [Adding properties to the `spark-defaults.conf` file in the `${SPARK_HOME}/conf` directory](#adding-properties-to-the-spark-defaultsconf-file-in-the-spark_homeconf-directory).

#### Setting the properties directly in your application

The below example demonstrates how to set the properties directly in your application when
constructing
a `SparkSession`.

:::warning
The setting `config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")` is
**extremely important**. Without it, the OpenLineage Spark integration will not be invoked, rendering
the integration ineffective.
:::

:::note 
Databricks For Databricks users, you must include `com.databricks.backend.daemon.driver.DBCEventLoggingListener` in addition to `io.openlineage.spark.agent.OpenLineageSparkListener` in the `spark.extraListeners` setting. Failure to do so will make the Spark UI inaccessible after a cluster shutdown.
:::

<Tabs groupId="spark-app-conf">
<TabItem value="scala" label="Scala">

```scala
import org.apache.spark.sql.SparkSession

object OpenLineageExample extends App {
  val spark = SparkSession.builder()
    .appName("OpenLineageExample")
    // This line is EXTREMELY important
    .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
    .config("spark.openlineage.transport.type", "http")
    .config("spark.openlineage.transport.url", "http://localhost:5000")
    .config("spark.openlineage.namespace", "spark_namespace")
    .config("spark.openlineage.parentJobNamespace", "airflow_namespace")
    .config("spark.openlineage.parentJobName", "airflow_dag.airflow_task")
    .config("spark.openlineage.parentRunId", "xxxx-xxxx-xxxx-xxxx")
    .getOrCreate()

  // ... your code

  spark.stop()
}

// For Databricks
import org.apache.spark.sql.SparkSession

object OpenLineageExample extends App {
  val spark = SparkSession.builder()
    .appName("OpenLineageExample")
    // This line is EXTREMELY important
    .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener,com.databricks.backend.daemon.driver.DBCEventLoggingListener")
    .config("spark.openlineage.transport.type", "http")
    .config("spark.openlineage.transport.url", "http://localhost:5000")
    .config("spark.openlineage.namespace", "spark_namespace")
    .config("spark.openlineage.parentJobNamespace", "airflow_namespace")
    .config("spark.openlineage.parentJobName", "airflow_dag.airflow_task")
    .config("spark.openlineage.parentRunId", "xxxx-xxxx-xxxx-xxxx")
    .getOrCreate()

  // ... your code

  spark.stop()
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder
    .appName("OpenLineageExample")
    .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
    .config("spark.openlineage.transport.type", "http")
    .config("spark.openlineage.transport.url", "http://localhost:5000")
    .config("spark.openlineage.namespace", "spark_namespace")
    .config("spark.openlineage.parentJobNamespace", "airflow_namespace")
    .config("spark.openlineage.parentJobName", "airflow_dag.airflow_task")
    .config("spark.openlineage.parentRunId", "xxxx-xxxx-xxxx-xxxx")
    .getOrCreate()

# ... your code

spark.stop()

# For Databricks
from pyspark.sql import SparkSession

spark = SparkSession.builder
    .appName("OpenLineageExample")
    .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener,com.databricks.backend.daemon.driver.DBCEventLoggingListener")
    .config("spark.openlineage.transport.type", "http")
    .config("spark.openlineage.transport.url", "http://localhost:5000")
    .config("spark.openlineage.namespace", "spark_namespace")
    .config("spark.openlineage.parentJobNamespace", "airflow_namespace")
    .config("spark.openlineage.parentJobName", "airflow_dag.airflow_task")
    .config("spark.openlineage.parentRunId", "xxxx-xxxx-xxxx-xxxx")
    .getOrCreate()

# ... your code

spark.stop()
```

</TabItem>
</Tabs>

#### Using `--conf` options with the CLI

The below example demonstrates how to use the `--conf` option with `spark-submit`.

:::note 
Databricks Remember to include `com.databricks.backend.daemon.driver.DBCEventLoggingListener` along with the OpenLineage listener. 
:::

```bash
spark-submit \
  --conf "spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener" \
  --conf "spark.openlineage.transport.type=http" \
  --conf "spark.openlineage.transport.url=http://localhost:5000" \
  --conf "spark.openlineage.namespace=spark_namespace" \
  --conf "spark.openlineage.parentJobNamespace=airflow_namespace" \
  --conf "spark.openlineage.parentJobName=airflow_dag.airflow_task" \
  --conf "spark.openlineage.parentRunId=xxxx-xxxx-xxxx-xxxx" \
  # ... other options

# For Databricks
spark-submit \
  --conf "spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener,com.databricks.backend.daemon.driver.DBCEventLoggingListener" \
  --conf "spark.openlineage.transport.type=http" \
  --conf "spark.openlineage.transport.url=http://localhost:5000" \
  --conf "spark.openlineage.namespace=spark_namespace" \
  --conf "spark.openlineage.parentJobNamespace=airflow_namespace" \
  --conf "spark.openlineage.parentJobName=airflow_dag.airflow_task" \
  --conf "spark.openlineage.parentRunId=xxxx-xxxx-xxxx-xxxx" \
  # ... other options
```

#### Adding properties to the `spark-defaults.conf` file in the `${SPARK_HOME}/conf` directory

:::warning
You may need to create this file if it does not exist. If it does exist, **we strongly suggest that
you back it up before making any changes**, particularly if you are not the only user of the Spark
installation. A misconfiguration here can have devastating effects on the operation of your Spark
installation, particularly in a shared environment.
:::

The below example demonstrates how to add properties to the `spark-defaults.conf` file.

:::note 
Databricks For Databricks users, include `com.databricks.backend.daemon.driver.DBCEventLoggingListener` in the `spark.extraListeners` property.
:::

```properties
spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener
spark.openlineage.transport.type=http
spark.openlineage.transport.url=http://localhost:5000
spark.openlineage.namespace=MyNamespace
```

For Databricks,
```properties
spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener,com.databricks.backend.daemon.driver.DBCEventLoggingListener
spark.openlineage.transport.type=http
spark.openlineage.transport.url=http://localhost:5000
spark.openlineage.namespace=MyNamespace
```

:::info
The `spark.extraListeners` configuration parameter is **non-additive**. This means that if you set
`spark.extraListeners` via the CLI or via `SparkSession#config`, it will **replace** the value
in `spark-defaults.conf`. This is important to remember if you are using `spark-defaults.conf` to
set a default value for `spark.extraListeners` and then want to override it for a specific job.
:::

:::info
When it comes to configuration parameters like `spark.openlineage.namespace`, a default value can
be supplied in the `spark-defaults.conf` file. This default value can be overridden by the
application at runtime, via the previously detailed methods. However, it is **strongly** recommended
that more dynamic or quickly changing parameters like `spark.openlineage.parentRunId` or
`spark.openlineage.parentJobName` be set at runtime via the CLI or `SparkSession#config` methods.
:::

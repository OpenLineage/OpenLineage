---
sidebar_position: 4
title: Scheduling from Airflow
---


The same parameters that are passed to `spark-submit` can also be supplied directly from **Airflow** 
and other schedulers, allowing for seamless configuration and execution of Spark jobs.

When using the [`OpenLineage Airflow`](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/index.html)
integration with operators that submit Spark jobs, the entire Spark OpenLineage integration can be configured 
directly within Airflow.

### Automatic Injection

There are several operators that are used to submit Spark jobs that in their newest versions have the 
ability to automatically inject the OpenLineage Spark integration into the Spark job.

There are two types of configuration that can be automatically injected: parent job info (see [Preserving Job Hierarchy](#preserving-job-hierarchy)) 
and transport info - that enables you to pass the same transport configuration from Airflow to the Spark job.

To enable configuring parent job info, Airflow configuration [spark_inject_parent_job_info](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/configurations-ref.html#spark-inject-parent-job-info)
must be set to true.

To enable configuring transport information, Airflow configuration [spark_inject_transport_info](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/configurations-ref.html#spark-inject-transport-info)
must be set to true.

See full [list of supported operators](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/supported_classes.html#spark-operators):

### Preserving Job Hierarchy

To establish a correct job hierarchy in lineage tracking, the Spark application and lineage backend require
identifiers of the parent job that triggered the Spark job. These identifiers allow the Spark integration
to automatically add a `ParentRunFacet` to the application-level OpenLineage event, facilitating the linkage 
of the Spark job to its originating (Airflow) job in the lineage graph.

The following properties are necessary for the automatic creation of the `ParentRunFacet`:

- `spark.openlineage.parentJobNamespace`
- `spark.openlineage.parentJobName`
- `spark.openlineage.parentRunId`

Additionally, in version 1.31.0 and later, the following properties are also added to `ParentRunFacet` that 
allow easier connection of the root (top-level parent) job to the children jobs:

- `spark.openlineage.rootParentJobNamespace`
- `spark.openlineage.rootParentJobName`
- `spark.openlineage.rootParentRunId`


Refer to the [Spark Configuration](spark_conf.md) documentation for more information on these properties.

OpenLineage Airflow integration provides powerful [macros](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/macros.html)
that can be used to dynamically generate these identifiers.

### Example

Below is an example of a `DataprocSubmitJobOperator` that submits a PySpark application to Dataproc cluster:

```python
t1 = DataprocSubmitJobOperator(
    task_id="task_id",
    project_id="project_id",
    region='eu-central2',
    job={
        "reference": {"project_id": "project_id"},
        "placement": {"cluster_name": "cluster_name"},
        "pyspark_job": {
            "main_python_file_uri": "gs://bucket/your-prog.py",
            "properties": {
                "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
                "spark.jars.packages": "io.openlineage:openlineage-spark_${SCALA_BINARY_VERSION}:{{PREPROCESSOR:OPENLINEAGE_VERSION}}",
                "spark.openlineage.transport.url": openlineage_url,
                "spark.openlineage.transport.auth.type": "api_key",
                "spark.openlineage.transport.auth.apiKey": api_key,
                "spark.openlineage.namespace": openlineage_spark_namespace,
                "spark.openlineage.parentJobNamespace": "{{ macros.OpenLineageProviderPlugin.lineage_job_namespace() }}",
                "spark.openlineage.parentJobName": "{{ macros.OpenLineageProviderPlugin.lineage_job_name(task_instance) }}",
                "spark.openlineage.parentRunId": "{{ macros.OpenLineageProviderPlugin.lineage_run_id(task_instance) }}",
            }
        },
    },
    dag=dag
)
```
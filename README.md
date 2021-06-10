# Marquez Spark Agent

The Marquez Spark Agent uses jvm instrumentation to emit OpenLineage metadata to Marquez.

## Installation

Maven:

```xml
<dependency>
    <groupId>io.github.marquezproject</groupId>
    <artifactId>marquez-spark</artifactId>
    <version>0.15.0</version>
</dependency>
```

or Gradle:

```groovy
implementation 'io.github.marquezproject:marquez-spark:0.15.0'
```

## Getting started

### Dataproc

Dataproc requires two things: a uri to the marquez java agent jar in the `files` parameter and
an additional spark property. Dataproc will copy the agent jar to the current working directory of the
executor and the `-javaagent` parameter will load it on execution.

```python
import os
...
job_name = 'job_name'

jar = 'marquez-spark-0.15.1-SNAPSHOT.jar'
files = [f"https://repo1.maven.org/maven2/io/github/marquezproject/marquez-spark/0.15.1-SNAPSHOT/marquez-spark-0.15.1-SNAPSHOT.jar"]

# Using the lineage_run_id macro in the airflow integration
t1 = DataProcPySparkOperator(
    task_id=job_name,
    gcp_conn_id='google_cloud_default',
    project_id='project_id',
    cluster_name='cluster-name',
    region='us-west1',
    main='gs://bucket/your-prog.py',
    job_name=job_name,
    dataproc_pyspark_properties={
      'spark.driver.extraJavaOptions':
        f"-javaagent:{jar}={os.environ.get('MARQUEZ_URL')}/api/v1/namespaces/{os.getenv('MARQUEZ_NAMESPACE', 'default')}/jobs/{job_name}/runs/{{{{lineage_run_id(run_id, task)}}}}?api_key={os.environ.get('MARQUEZ_API_KEY')}"
    files=files,
    dag=dag)
```

## Arguments

The java agent accepts an argument in the form of a uri. It includes the location of Marquez, the
namespace name, the job name, and a unique run id. The run id will be emitted as a parent run
facet.
```
{marquez_home}/api/v1/namespaces/{namespace}/job/{job_name}/runs/{run_uuid}?api_key={api_key}"

```
For example:
```
https://marquez.example.com:5000/api/v1/namespaces/foo/job/spark.submit_job/runs/a95858ad-f9b5-46d7-8f1c-ca9f58f68978"
```

# Compatibility Notes
Tested and compatible for Spark `2.4.7` only. Other spark versions may cause the spark agent to throw class loading exception errors.

# Build

## Java 8

Testing requires a Java 8 JVM to test the scala spark components.

`export JAVA_HOME=`/usr/libexec/java_home -v 1.8`

## Testing

To run the tests, from the root directory run:

```sh
./gradlew :integrations:spark:test
```

## Build spark agent jar

```sh
./gradlew :integrations:spark:shadowJar
```

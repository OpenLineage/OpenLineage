# Marquez Spark Agent

The Marquez Spark Agent uses jvm instrumentation to emit OpenLineage metadata to Marquez.

## Installation

Maven:

```xml
<dependency>
    <groupId>io.github.marquezproject</groupId>
    <artifactId>marquez-spark</artifactId>
    <version>0.15.2-rc.3</version>
</dependency>
```

or Gradle:

```groovy
implementation 'io.github.marquezproject:marquez-spark:0.15.2-rc.3
```

## Getting started

### Quickstart
The fastest way to get started testing Spark and Marquez is to use the docker-compose files included
in the project. From the spark integration directory ($MARQUEZ_ROOT/integrations/spark) execute
```bash
docker-compose up
```
This will start the Marquez service on localhost:5000, the web UI on localhost:3000, and a Jupyter
Spark notebook on localhost:8888. On startup, the notebook container logs will show a list of URLs 
including an access token, such as
```bash
notebook_1  |     To access the notebook, open this file in a browser:
notebook_1  |         file:///home/jovyan/.local/share/jupyter/runtime/nbserver-9-open.html
notebook_1  |     Or copy and paste one of these URLs:
notebook_1  |         http://abc12345d6e:8888/?token=XXXXXX
notebook_1  |      or http://127.0.0.1:8888/?token=XXXXXX
```
Copy the URL with the localhost IP and paste into your browser window to begin creating a new Jupyter
Spark notebook (see the [https://jupyter-docker-stacks.readthedocs.io/en/latest/](docs) for info on 
using the Jupyter docker image). 

#### Marquez 0.15.1 and higher
Starting with Marquez 0.15.1, the SparkListener can be referenced as a plain Spark Listener implementation.
This makes setup very simple and easy to understand.

Create a new notebook and paste the following into the first cell:
```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder.master('local')
         .appName('sample_spark')
         .config('spark.jars.packages', 'io.github.marquezproject:marquez-spark:0.15.2-rc.3
         .config('spark.extraListeners', 'marquez.spark.agent.SparkListener')
         .config('openlineage.url', 'http://marquez:5000/api/v1/namespaces/spark_integration/')
         .getOrCreate())
```
To use the local jar, you can build it with 
```bash
gradle shadowJar
```
then reference it in the Jupyter notebook with the following (note that the jar should be built 
*before* running the `docker-compose up` step or docker will just mount a dummy folder; once the 
`build/libs` directory exists, you can repeatedly build the jar without restarting the jupyter 
container):
```python
from pyspark.sql import SparkSession

file = "/home/jovyan/marquezlib/libs/marquez-spark-0.15.1-SNAPSHOT.jar"

spark = (SparkSession.builder.master('local').appName('rdd_to_dataframe')
             .config('spark.jars', file)
             .config('spark.jars.packages', 'org.postgresql:postgresql:42.2.+')
             .config('spark.extraListeners', 'marquez.spark.agent.SparkListener')
             .config('openlineage.url', 'http://marquez:5000/api/v1/namespaces/spark_integration/')
             .getOrCreate())
```

#### Prior to Marquez 0.15.1
Prior to Marquez 0.15.1, the SparkListener only worked as a java agent that needs to be added to 
the JVM startup parameters. Setup in a pyspark notebook looks like the following:

```python
from pyspark.sql import SparkSession

file = "/home/jovyan/marquezlib/libs/marquez-spark-0.15.1-SNAPSHOT.jar"

spark = (SparkSession.builder.master('local').appName('rdd_to_dataframe')
         .config('spark.driver.extraJavaOptions',
                 f"-javaagent:{file}=http://marquez:5000/api/v1/namespaces/spark_integration/")
         .config('spark.jars.packages', 'org.postgresql:postgresql:42.2.+')
         .config('spark.sql.repl.eagerEval.enabled', 'true')
         .getOrCreate())
```
When running on a real cluster, the marquez-spark jar has to be in a known location on the master 
node of the cluster and its location referenced in the `spark.driver.extraJavaOptions` parameter.

### Dataproc Airflow Example
#### Using the SparkListener (Marquez 0.15.1 and higher)
A Dataproc operator needs the `marquez-spark` package specified in the `spark.jars.packages` 
configuration parameter and the OpenLineage server URL in the Spark configuration.
```python
import os
...
job_name = 'job_name'

jar = 'marquez-spark-0.15.0.jar'
files = [f"https://repo1.maven.org/maven2/io/github/marquezproject/marquez-spark/0.15.0/marquez-spark-0.15.0.jar"]

marquez_url = os.environ.get('MARQUEZ_URL')
marquez_namespace = os.getenv('MARQUEZ_NAMESPACE', 'default')
api_key = os.environ.get('MARQUEZ_API_KEY')

t1 = DataProcPySparkOperator(
    task_id=job_name,
    gcp_conn_id='google_cloud_default',
    project_id='project_id',
    cluster_name='cluster-name',
    region='us-west1',
    main='gs://bucket/your-prog.py',
    job_name=job_name,
    dataproc_pyspark_properties={
      "spark.extraListeners": "marquez.spark.agent.SparkListener",
      "spark.jars.packages": "io.github.marquezproject:marquez-spark:0.15.2-rc.3
      "openlineage.url": f"{marquez_url}/api/v1/namespaces/{marquez_namespace}/jobs/dump_orders_to_gcs/runs/{{{{task_run_id(run_id, task)}}}}?api_key={api_key}"
    },
    dag=dag)
```

Alternatively, the Dataproc cluster can be created with the listener configuration in the Spark
properties.

```python
import os
...
job_name = 'job_name'

marquez_url = os.environ.get('MARQUEZ_URL')
marquez_namespace = os.getenv('MARQUEZ_NAMESPACE', 'default')
api_key = os.environ.get('MARQUEZ_API_KEY')

t1 = DataprocClusterCreateOperator(
    task_id='create_dataproc_cluster',
    cluster_name='spark-airflow-bq',
    gcp_conn_id=GCP_CONN_ID,
    project_id=GCP_PROJECT_ID,
    region=GCP_REGION,
    zone=GCP_ZONE,
    num_masters=1,
    num_workers=2,
    master_machine_type='n1-standard-4',
    worker_machine_type='n1-standard-4',
    image_version='1.4-debian10',
    init_actions_uris=['gs://dataproc-initialization-actions/cloud-sql-proxy/cloud-sql-proxy.sh'],
    properties={
      "spark.extraListeners": "marquez.spark.agent.SparkListener",
      "spark.jars.packages": "io.github.marquezproject:marquez-spark:0.15.2-rc.3
      "openlineage.url": "{marquez_url}/api/v1/namespaces/{marquez_namespace}/?api_key={api_key}"
    },
    dag=dag)
```
#### Using the Javaagent (Marquez 0.15.0 and earlier)
Dataproc requires two things: a uri to the marquez java agent jar in the `files` parameter and
an additional spark property. Dataproc will copy the agent jar to the current working directory of the
executor and the `-javaagent` parameter will load it on execution.

```python
import os
...
job_name = 'job_name'

jar = 'marquez-spark-0.15.1-SNAPSHOT.jar'
files = [f"https://repo1.maven.org/maven2/io/github/marquezproject/marquez-spark/0.15.1-SNAPSHOT/marquez-spark-0.15.1-SNAPSHOT.jar"]

marquez_url = os.environ.get('MARQUEZ_URL')
marquez_namespace = os.getenv('MARQUEZ_NAMESPACE', 'default')
api_key = os.environ.get('MARQUEZ_API_KEY')

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
        f"-javaagent:{jar}={marquez_url}/api/v1/namespaces/{marquez_namespace}/jobs/dump_orders_to_gcs/runs/{{{{lineage_run_id(run_id, task)}}}}?api_key={api_key}"
    files=files,
    dag=dag)
```

## Arguments

### Spark Listener
The SparkListener reads its configuration from SparkConf parameters. These can be specified on the
command line (e.g., `--conf "openlineage.url=http://marquez:5000/api/v1/namespaces/my_namespace/job/the_job"`)
or from the `conf/spark-defaults.conf` file. The following parameters can be specified
| Parameter | Definition | Example |
| openlineage.host | The hostname of the OpenLineage API server where events should be reported | http://localhost:5000 |
| openlineage.version | The API version of the OpenLineage API server | 1|
| openlineage.namespace | The default namespace to be applied for any jobs submitted | MyNamespace|
| openlineage.parentJobName | The job name to be used for the parent job facet | ParentJobName | 
| openlineage.parentRunId | The RunId of the parent job that initiated this Spark job | xxxx-xxxx-xxxx-xxxx |
| openlineage.apiKey | An API key to be used when sending events to the OpenLineage server | abcdefghijk |

### Java Agent
The java agent accepts an argument in the form of a uri. It includes the location of Marquez, the
namespace name, the parent job name, and a parent run id. The run id will be emitted as a parent run
facet.
```
{marquez_home}/api/v1/namespaces/{namespace}/job/{job_name}/runs/{run_uuid}?api_key={api_key}"

```
For example:
```
https://marquez.example.com:5000/api/v1/namespaces/foo/job/spark.submit_job/runs/a95858ad-f9b5-46d7-8f1c-ca9f58f68978"
```

# Compatibility Notes
Tested and compatible for Spark `2.4.7` only.

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

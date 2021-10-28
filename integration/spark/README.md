# OpenLineage Spark Agent

The OpenLineage Spark Agent uses jvm instrumentation to emit OpenLineage metadata.

## Installation

Maven:

```xml
<dependency>
    <groupId>io.openlineage</groupId>
    <artifactId>openlineage-spark</artifactId>
    <version>0.3.1</version>
</dependency>
```

or Gradle:

```groovy
implementation 'io.openlineage:openlineage-spark:0.3.1'
```

## Getting started

### Quickstart
The fastest way to get started testing Spark and OpenLineage is to use the docker-compose files included
in the project. From the spark integration directory ($OPENLINEAGE_ROOT/integration/spark) execute
```bash
docker-compose up
```
This will start Marquez as an Openlineage client and Jupyter Spark notebook on localhost:8888. On startup, the notebook container logs will show a list of URLs
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

# OpenLineageSparkListener as a plain Spark Listener
The SparkListener can be referenced as a plain Spark Listener implementation.

Create a new notebook and paste the following into the first cell:
```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder.master('local')
         .appName('sample_spark')
         .config('spark.jars.packages', 'io.openlineage:openlineage-spark:0.3.1')
         .config('spark.extraListeners', 'io.openlineage.spark.agent.OpenLineageSparkListener')
         .config('spark.openlineage.url', 'http://{openlineage.client.host}/api/v1/namespaces/spark_integration/')
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

file = "/home/jovyan/openlineage/libs/openlineage-spark-0.3.1.jar"

spark = (SparkSession.builder.master('local').appName('rdd_to_dataframe')
             .config('spark.jars', file)
             .config('spark.jars.packages', 'org.postgresql:postgresql:42.2.+')
             .config('spark.extraListeners', 'io.openlineage.spark.agent.OpenLineageSparkListener')
             .config('spark.openlineage.url', 'http://{openlineage.client.host}/api/v1/namespaces/spark_integration/')
             .getOrCreate())
```

# OpenLineageSparkListener as a java agent
Configuring SparkListener as a java agent that needs to be added to
the JVM startup parameters. Setup in a pyspark notebook looks like the following:

```python
from pyspark.sql import SparkSession

file = "/home/jovyan/openlineage/libs/openlineage-spark-0.3.1.jar"

spark = (SparkSession.builder.master('local').appName('rdd_to_dataframe')
         .config('spark.driver.extraJavaOptions',
                 f"-javaagent:{file}=http://{openlineage.client.host}/api/v1/namespaces/spark_integration/")
         .config('spark.jars.packages', 'org.postgresql:postgresql:42.2.+')
         .config('spark.sql.repl.eagerEval.enabled', 'true')
         .getOrCreate())
```
When running on a real cluster, the openlineage-spark jar has to be in a known location on the master
node of the cluster and its location referenced in the `spark.driver.extraJavaOptions` parameter.
## Arguments

### Spark Listener
The SparkListener reads its configuration from SparkConf parameters. These can be specified on the
command line (e.g., `--conf "spark.openlineage.url=http://{openlineage.client.host}/api/v1/namespaces/my_namespace/job/the_job"`)
or from the `conf/spark-defaults.conf` file. 

The following parameters can be specified
| Parameter | Definition | Example |
------------|------------|---------
| spark.openlineage.host | The hostname of the OpenLineage API server where events should be reported | http://localhost:5000 |
| spark.openlineage.version | The API version of the OpenLineage API server | 1|
| spark.openlineage.namespace | The default namespace to be applied for any jobs submitted | MyNamespace|
| spark.openlineage.parentJobName | The job name to be used for the parent job facet | ParentJobName |
| spark.openlineage.parentRunId | The RunId of the parent job that initiated this Spark job | xxxx-xxxx-xxxx-xxxx |
| spark.openlineage.apiKey | An API key to be used when sending events to the OpenLineage server | abcdefghijk |

### Java Agent
The java agent accepts an argument in the form of a uri. It includes the location of OpenLineage client, the
namespace name, the parent job name, and a parent run id. The run id will be emitted as a parent run
facet.
```
{openlineage.client.host}/api/v1/namespaces/{namespace}/job/{job_name}/runs/{run_uuid}?api_key={api_key}"

```
For example:
```
https://openlineage.client.host/api/v1/namespaces/foo/job/spark.submit_job/runs/a95858ad-f9b5-46d7-8f1c-ca9f58f68978"
```

# Build

## Java 8

Testing requires a Java 8 JVM to test the scala spark components.

`export JAVA_HOME=`/usr/libexec/java_home -v 1.8`

## Testing

To run the tests, from the current directory run:

```sh
./gradlew test
```

To run the integration tests, from the current directory run:

```sh
./gradlew integrationTest
```

## Build spark agent jar

```sh
./gradlew shadowJar
```

---
title: Testing
sidebar_position: 11
---

# Testing

## Configurable Integration Test

Starting of version 1.17, OpenLineage Spark integration provides a command line tooling to help
creating custom integration tests. `configurable-test.sh` script can be used to build
`openlineage-spark` from the current directory, script arguments are used to pass Spark
job. Then, emitted OpenLineage events are validated against JSON files with expected events' fields. Build process and 
integration test run itself is performed within Docker environment which makes the command  
Java environment agnostic. 

:::info
Quickstart: try running following command from OpenLineage project root directory: 
```bash
./integration/spark/cli/configurable-test.sh --spark ./integration/spark/cli/spark-conf.yml --test ./integration/spark/cli/tests
```
This should run four integration tests `./integration/spark/cli/tests` and store their output into
`./integration/spark/cli/runs`. Feel free to add extra test directories with custom tests.  
::: 

What's happening when running  `configurable-test.sh` command? 
 * At first, a docker container with Java 11 is created. It builds a docker image `openlineage-test:$OPENLINEAGE_VERSION`. During the build process, all the internal dependencies (like `openlineage-java`) are added to the image. It's because we don't want to build it in each run as it speeds up single command run. In case of subproject changes, a new image has to be built.
 * Once the docker image is built, docker container is started and starts gradle `configurableIntegrationTest` task. Task depends on `shadowJar` to build `openlineage-spark` jar. The built jar should be also available on host machine. 
 * Gradle test task spawns additional Spark containers which run the Spark job and emit OpenLineage events to local file. A gradle test code has access to mounted event file location, fetches the events emitted and verifies them against expected JSON events. Matching is done through MockServer Json body matching with `ONLY_MATCHING_FIELDS` flag set, as it's happening within other integration tests.
 * Test output is written into `./integration/spark/cli/runs` directories with subdirectories containing test definition and file with events that was emitted. 

:::info
Please be aware that first run of the command will download several gigabytes of docker images being used 
as well as gradle dependencies required to build JAR from the source code. All of them are stored
within Docker volumes, which makes consecutive runs a way faster. 
:::

### Command details

It is important to run command from the project root directory. This is the only way to let 
created Docker containers get mounted volumes containing spark integration code, java client code,
sql integration code. Command has extra check to verify if work directory is correct.

Try running:
```bash
./integration/spark/cli/configurable-test.sh --help
```
to see all the options available within your version. These should include:
 * `--spark` - to define spark environment configuration file,
 * `--test` - location for the directory containing tests,
 * `--clean` - flague marking docker image to be re-build from scratch.

### Spark configuration file 

This an example Spark environment configuration file:
```yaml
appName: "CLI test application"
sparkVersion: 3.3.4
scalaBinaryVersion: 2.12
enableHiveSupport: true
packages:
  - org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.2
sparkConf:
  spark.openlineage.debugFacet.disabled: false
```

* `sparkVersion` and `scalaBinaryVersion` are used to determine Spark and Scala version to be tested. Spark is run on docker from the images available in 
[https://quay.io/repository/openlineage/spark?tab=tags](https://quay.io/repository/openlineage/spark?tab=tags). A combination of Spark and Scala version provided within
the config has to match images available.
* `appName` and `enableHiveSupport` parameters are used when starting Spark session.
* `sparkConf` can be used to pass any spark configuration entries. OpenLineage transport defined is file based with a specified file location and is set within the test being run. Those settings should not be overrider. 
* `packages` lets define custom jar packages to be installed with `spark-submit` command. 

As of version 1.18, Spark configuration can accept instead of `sparkVersion`, a configuration 
entries to determine Docker image to be run on:
```yaml
appName: "CLI test application"
docker:
  image: "apache/spark:3.3.3-scala2.12-java11-python3-ubuntu"
  sparkSubmit: /opt/spark/bin/spark-submit
  waitForLogMessage: ".*ShutdownHookManager: Shutdown hook called.*"
scalaBinaryVersion: 2.12
```
where: 
 * `image` specifies docker image to be used to run Spark job,
 * `sparkSubmit` is file location of `spark-submit` command,
 * `waitForLogMessage` is regex for log entry determining a Spark job is finished. 

### Tests definition directories

 * Specified test directory should contain one or more directories and each of the subdirectories contains separate test definition. 
 * Each test directory should contain a single `.sql` or `.py` pySpark code file containing a job definition. For `.sql` file each line of the file is decorated with `spark.sql()` and transformed into pySpark script. 
For pySpark scripts, a user should instantiate SparkSession with OpenLineage parameters configured properly. Please refer to existing tests for usage examples. 
 * Each test directory should contain on or more event definition file with `.json` extensions defining an expected content of any of the events emitted by the job run. 

# OpenLineage Airflow Integration

A library that integrates [Airflow `DAGs`]() with [OpenLineage](https://openlineage.io) for automatic metadata collection.

## Features

**Metadata**

* Task lifecycle
* Task parameters
* Task runs linked to **versioned** code
* Task inputs / outputs

**Lineage**

* Track inter-DAG dependencies

**Built-in**

* SQL parser
* Link to code builder (ex: **GitHub**)
* Metadata extractors

## Requirements

- [Python 3.7](https://www.python.org/downloads)
- [Airflow 1.10.12+](https://pypi.org/project/apache-airflow)
- (experimental) [Airflow 2.1+](https://pypi.org/project/apache-airflow)

## Installation

```bash
$ pip3 install openlineage-airflow
```

> **Note:** You can also add `openlineage-airflow` to your `requirements.txt` for Airflow.

To install from source, run:

```bash
$ python3 setup.py install
```

## Setup

### Airflow 2.3+

Integration automatically registers itself for Airflow 2.3 if it's installed on Airflow worker's python.
This means you don't have to do anything besides configuring it, which is described in Configuration section.

### Airflow 2.1 - 2.2

This method has limited support: 
it does not support tracking failed jobs, and job starts are registered only when job ends.

Set your LineageBackend in your [airflow.cfg](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-config.html) or via environmental variable `AIRFLOW__LINEAGE__BACKEND`
to 
```
openlineage.lineage_backend.OpenLineageBackend
```

In contrast to integration via subclassing `DAG`, `LineageBackend` based approach collects all metadata 
for task on each task completion.

OpenLineageBackend does not take into account manually configured inlets and outlets. 

### Airflow 1.10+

To begin collecting Airflow DAG metadata with OpenLineage, use:

```diff
- from airflow import DAG
+ from openlineage.airflow import DAG
```

When enabled, the library will:

1. On DAG **start**, collect metadata for each task using an `Extractor` if it exists for given operator.
2. Collect task input / output metadata (`source`, `schema`, etc)
3. Collect task run-level metadata (execution time, state, parameters, etc)
4. On DAG **complete**, also mark the task as _complete_ in OpenLineage

## Configuration

### `HTTP` Backend Environment Variables

`openlineage-airflow` uses OpenLineage client to push data to OpenLineage backend.

OpenLineage client depends on environment variables:

* `OPENLINEAGE_URL` - point to service which will consume OpenLineage events
* `OPENLINEAGE_API_KEY` - set if consumer of OpenLineage events requires `Bearer` authentication key
* `OPENLINEAGE_NAMESPACE` - set if you are using something other than the `default` namespace for job namespace.
* `OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE` - set to `False` if you want your source code of callables provided in PythonOperator to be send in OpenLineage events

For backwards compatibility, `openlineage-airflow` also support configuration via
`MARQUEZ_URL`, `MARQUEZ_NAMESPACE` and `MARQUEZ_API_KEY` variables.

```
MARQUEZ_URL=http://my_hosted_marquez.example.com:5000
MARQUEZ_NAMESPACE=my_special_ns
```

### Extractors : Sending the correct data from your DAGs

If you do nothing, OpenLineage backend will receive the `Job` and the `Run` from your DAGs, but,
unless you use few operators for which this integration provides extractor, input and output metadata will not be send.

`openlineage-airflow` allows you to do more than that by building "Extractors". Extractor is object
suited to extract metadata from particular operator (or operators). 

1. Name : The name of the task
2. Inputs : List of input datasets
3. Outputs : List of output datasets
4. Context : The Airflow context for the task

#### Bundled Extractors

`openlineage-airflow` provides extractors for

* `PostgresOperator`
* `MySqlOperator`
* `BigQueryOperator`
* `SnowflakeOperator`
* `GreatExpectationsOperator`
* `PythonOperator`

SQL Operators utilize SQL parser. There is experimental SQL parser, activated if you install [openlineage-sql](https://pypi.org/project/openlineage-sql) on your Airflow worker.

#### Custom Extractors

If your DAGs contain additional operators from which you want to extract lineage data, fear not - you can always
provide custom extractors. They should derive from `BaseExtractor`. 

There are two ways to register them for use in `openlineage-airflow`. 

First one, is to add them to `OPENLINEAGE_EXTRACTORS` environment variable, separated by comma `(;)` 
```
OPENLINEAGE_EXTRACTORS=full.path.to.ExtractorClass;full.path.to.AnotherExtractorClass
```

Second one - working in Airflow 1.10.x only - is to register all additional operator-extractor pairings by 
providing `lineage_custom_extractors` argument in `openlineage.airflow.DAG`.

#### Great Expectations

Great Expectations integration works by providing OpenLineageValidationAction. You need to include it into your `action_list` in `great_expectations.yml`.

The following example illustrates example change in default configuration: 
```diff
validation_operators:
  action_list_operator:
    # To learn how to configure sending Slack notifications during evaluation
    # (and other customizations), read: https://docs.greatexpectations.io/en/latest/autoapi/great_expectations/validation_operators/index.html#great_expectations.validation_operators.ActionListValidationOperator
    class_name: ActionListValidationOperator
    action_list:
      - name: store_validation_result
        action:
          class_name: StoreValidationResultAction
      - name: store_evaluation_params
        action:
          class_name: StoreEvaluationParametersAction
      - name: update_data_docs
        action:
          class_name: UpdateDataDocsAction
+     - name: openlineage
+       action:
+         class_name: OpenLineageValidationAction
+         module_name: openlineage.common.provider.great_expectations.action
      # - name: send_slack_notification_on_validation_result
      #   action:
      #     class_name: SlackNotificationAction
      #     # put the actual webhook URL in the uncommitted/config_variables.yml file
      #     slack_webhook: ${validation_notification_slack_webhook}
      #     notify_on: all # possible values: "all", "failure", "success"
      #     renderer:
      #       module_name: great_expectations.render.renderer.slack_renderer
      #       class_name: SlackRenderer
```

If you're using `GreatExpectationsOperator`, you need to set `validation_operator_name` to operator that includes OpenLineageValidationAction.
Setting it in `great_expectations.yml` files isn't enough - the operator overrides it with default name if it's not provided.

To see example of working configuration, you can see [DAG](https://github.com/OpenLineage/OpenLineage/blob/main/integration/airflow/tests/integration/airflow/dags/greatexpectations_dag.py) and [Great Expectations configuration](https://github.com/OpenLineage/OpenLineage/tree/main/integration/airflow/tests/integration/data/great_expectations) in integration tests.

## Triggering Child Jobs
Commonly, Airflow DAGs will trigger processes on remote systems, such as an Apache Spark or Apache
Beam job. Those systems may have their own OpenLineage integration and report their own
job runs and dataset inputs/outputs. To propagate the job hierarchy, tasks must send their own run
id so that the downstream process can report the [ParentRunFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json#/definitions/ParentRunFacet)
with the proper run id.

The `lineage_run_id` and `lineage_parent_id` macros exists to inject the run id or whole parent run information
of a given task into the arguments sent to a  remote processing job's Airflow operator. The macro requires the 
DAG run_id and the task to access the generated run id for that task. For example, a Spark job can be triggered
using the `DataProcPySparkOperator` with the correct parent run id using the following configuration:

Airflow 1.10:

```python
t1 = DataProcPySparkOperator(
    task_id=job_name,
    #required pyspark configuration,
    job_name=job_name,
    dataproc_pyspark_properties={
        'spark.driver.extraJavaOptions':
            f"-javaagent:{jar}={os.environ.get('OPENLINEAGE_URL')}/api/v1/namespaces/{os.getenv('OPENLINEAGE_NAMESPACE', 'default')}/jobs/{job_name}/runs/{{{{lineage_run_id(run_id, task)}}}}?api_key={os.environ.get('OPENLINEAGE_API_KEY')}"
        dag=dag)
```

Airflow 2.0+:

```python
t1 = DataProcPySparkOperator(
    task_id=job_name,
    #required pyspark configuration,
    job_name=job_name,
    dataproc_pyspark_properties={
        'spark.driver.extraJavaOptions':
            f"-javaagent:{jar}={os.environ.get('OPENLINEAGE_URL')}/api/v1/namespaces/{os.getenv('OPENLINEAGE_NAMESPACE', 'default')}/jobs/{job_name}/runs/{{{{macros.OpenLineagePlugin.lineage_run_id(run_id, task)}}}}?api_key={os.environ.get('OPENLINEAGE_API_KEY')}"
        dag=dag)
```

## Development

To install all dependencies for _local_ development:

Airflow integration depends on `openlineage.sql`, `openlineage.common` & `openlineage.client.python`. You should install them first independently or try to install with following command:

Airflow 1.10:
```bash
$ pip install -r dev-requirements-1.x.txt
```

Airflow 2.0+:
```bash
$ pip install -r dev-requirements-2.x.txt
```

There is also bash script that can run arbitrary Airflow image with OpenLineage integration build from current branch.
Run it as
```bash
$ AIRFLOW_IMAGE=<airflow_image_with_tag> ./scripts/run-dev-airflow.sh [--rebuild]
```
Rebuild option forces docker images to be rebuilt.
### Unit tests
To run the entire unit test suite use below command:
```bash
$ tox
```
or choose one of the environments, e.g.:
```bash
$ tox -e py-airflow214
```
You can also skip using `tox` and run `pytest` on your own dev environment.
### Integration tests
Integration tests require usage of _docker compose_. There are scripts prepared to make build images and run tests easier.

Airflow 1.10:
```bash
$ ./tests/integration/docker/up.sh
```
Airflow 2.0+:
```bash
$ AIRFLOW_IMAGE=<name-of-airflow-image> ./tests/integration/docker/up-2.sh
```
e.g.
```bash
$ AIRFLOW_IMAGE=apache/airflow:2.3.1-python3.7 ./tests/integration/docker/up-2.sh
```
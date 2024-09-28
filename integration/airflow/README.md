# OpenLineage Airflow Integration

A library that integrates [Airflow `DAGs`]() with [OpenLineage](https://openlineage.io) for automatic metadata collection.

## Native integration with Airflow

Starting from Airflow version 2.7.0 OpenLineage integration is included in Airflow repository as a provider.
The `apache-airflow-providers-openlineage` 
[package](https://airflow.apache.org/docs/apache-airflow-providers-openlineage) 
significantly ease lineage tracking in Airflow, 
ensuring stability by embedding the functionality directly into each provider and 
simplifying the process for users to select and manage lineage collection consumers.

As a result, **starting from Airflow 2.7.0** one should use the native Airflow Openlineage provider 
[package](https://airflow.apache.org/docs/apache-airflow-providers-openlineage).  

The ongoing development and enhancements will be focused on the `apache-airflow-providers-openlineage` package, 
while the `openlineage-airflow` will primarily be updated for bug fixes.

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

- [Python 3.8](https://www.python.org/downloads)
- [Airflow >= 2.1,<2.8](https://pypi.org/project/apache-airflow)

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

### Airflow 2.7+

This package **should not** be used starting with Airflow 2.7.0 and **must not** be used with Airflow 2.8+. 
It was designed as Airflow's external integration that works mainly for Airflow versions <2.7.
For Airflow 2.7+ use the native Airflow OpenLineage provider 
[package](https://airflow.apache.org/docs/apache-airflow-providers-openlineage) `apache-airflow-providers-openlineage`.

### Airflow 2.3 - 2.6

The integration automatically registers itself starting from Airflow 2.3 if it's installed on the Airflow worker's Python.
This means you don't have to do anything besides configuring it, which is described in the Configuration section.

### Airflow 2.1 - 2.2

> **Note:** As of version 1.15.0, Airflow versions below 2.3.0 are no longer supported. The description below pertains to earlier versions.

This method has limited support: it does not support tracking failed jobs, and job starts are registered only when a job ends.

Set your LineageBackend in your [airflow.cfg](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-config.html) or via environmental variable `AIRFLOW__LINEAGE__BACKEND` to 
```
openlineage.lineage_backend.OpenLineageBackend
```

In contrast to integration via subclassing a `DAG`, a `LineageBackend`-based approach collects all metadata 
for a task on each task's completion.

The OpenLineageBackend does not take into account manually configured inlets and outlets. 

When enabled, the library will:

1. On DAG **start**, collect metadata for each task using an `Extractor` if it exists for a given operator.
2. Collect task input / output metadata (`source`, `schema`, etc.)
3. Collect task run-level metadata (execution time, state, parameters, etc.)
4. On DAG **complete**, also mark the task as _complete_ in OpenLineage

## Configuration

### `HTTP` Backend Environment Variables

`openlineage-airflow` uses the OpenLineage client to push data to OpenLineage backend.

The OpenLineage client depends on environment variables:

* `OPENLINEAGE_URL` - point to the service that will consume OpenLineage events.
* `OPENLINEAGE_API_KEY` - set if the consumer of OpenLineage events requires a `Bearer` authentication key.
* `OPENLINEAGE_NAMESPACE` - set if you are using something other than the `default` namespace for the job namespace.
* `OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE` - set to `False` if you want the source code of callables provided in the PythonOperator to be sent in OpenLineage events.

For backwards compatibility, `openlineage-airflow` also supports configuration via
`MARQUEZ_URL`, `MARQUEZ_NAMESPACE` and `MARQUEZ_API_KEY` variables.

```
MARQUEZ_URL=http://my_hosted_marquez.example.com:5000
MARQUEZ_NAMESPACE=my_special_ns
```

### Extractors : Sending the correct data from your DAGs

If you do nothing, the OpenLineage backend will receive the `Job` and the `Run` from your DAGs, but,
unless you use one of the few operators for which this integration provides an extractor, input and output metadata will not be sent.

`openlineage-airflow` allows you to do more than that by building "Extractors." An extractor is an object
suited to extract metadata from a particular operator (or operators). 

1. Name : The name of the task
2. Inputs : A list of input datasets
3. Outputs : A list of output datasets
4. Context : The Airflow context for the task

#### Bundled Extractors

`openlineage-airflow` provides extractors for:

* `PostgresOperator`
* `MySqlOperator`
* `AthenaOperator`
* `BigQueryOperator`
* `SnowflakeOperator`
* `TrinoOperator`
* `GreatExpectationsOperator`
* `SFTPOperator`
* `FTPFileTransmitOperator`
* `PythonOperator`
* `RedshiftDataOperator`, `RedshiftSQLOperator`
* `SageMakerProcessingOperator`, `SageMakerProcessingOperatorAsync`
* `SageMakerTrainingOperator`, `SageMakerTrainingOperatorAsync`
* `SageMakerTransformOperator`, `SageMakerTransformOperatorAsync`
* `S3CopyObjectExtractor`, `S3FileTransformExtractor`
* `GCSToGCSOperator`
* `DbtCloudRunJobOperator`

SQL Operators utilize the SQL parser. There is an experimental SQL parser activated if you install [openlineage-sql](https://pypi.org/project/openlineage-sql) on your Airflow worker.


#### Custom Extractors

If your DAGs contain additional operators from which you want to extract lineage data, fear not - you can always
provide custom extractors. They should derive from `BaseExtractor`. 

There are two ways to register them for use in `openlineage-airflow`. 

One way is to add them to the `OPENLINEAGE_EXTRACTORS` environment variable, separated by a semi-colon `(;)`. 
```
OPENLINEAGE_EXTRACTORS=full.path.to.ExtractorClass;full.path.to.AnotherExtractorClass
```

To ensure OpenLineage logging propagation to custom extractors you should use `self.log` instead of creating a logger yourself.

#### Default Extractor

When you own operators' code this is not necessary to provide custom extractors. You can also use Default Extractor's capability.

In order to do that you should define at least one of two methods in operator:

* `get_openlineage_facets_on_start()`

Extracts metadata on start of task.

* `get_openlineage_facets_on_complete(task_instance: TaskInstance)`

Extracts metadata on complete of task. This should accept `task_instance` argument, similar to `extract_on_complete` method in base extractors.

If you don't define `get_openlineage_facets_on_complete` method it would fall back to `get_openlineage_facets_on_start`.


#### Great Expectations

The Great Expectations integration works by providing an OpenLineageValidationAction. You need to include it into your `action_list` in `great_expectations.yml`.

The following example illustrates a way to change the default configuration: 
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

If you're using `GreatExpectationsOperator`, you need to set `validation_operator_name` to an operator that includes OpenLineageValidationAction.
Setting it in `great_expectations.yml` files isn't enough - the operator overrides it with the default name if a different one is not provided.

To see an example of a working configuration, see [DAG](https://github.com/OpenLineage/OpenLineage/blob/main/integration/airflow/tests/integration/tests/airflow/dags/greatexpectations_dag.py) and [Great Expectations configuration](https://github.com/OpenLineage/OpenLineage/tree/main/integration/airflow/tests/integration/data/great_expectations) in the integration tests.

### Logging
In addition to conventional logging approaches, the `openlineage-airflow` package provides an alternative way of configuring its logging behavior. By setting the `OPENLINEAGE_AIRFLOW_LOGGING` environment variable, you can establish the logging level for the `openlineage.airflow` and its child modules.

## Triggering Child Jobs

Commonly, Airflow DAGs will trigger processes on remote systems, such as an Apache Spark or Apache
Beam job. Those systems may have their own OpenLineage integrations and report their own
job runs and dataset inputs/outputs. To propagate the job hierarchy, tasks must send their own run
ids so that the downstream process can report the [ParentRunFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json#/definitions/ParentRunFacet)
with the proper run id.

These macros:
  * `lineage_job_namespace()`
  * `lineage_job_name(task_instance)`
  * `lineage_run_id(task_instance)`

allow injecting pieces of run information of a given Airflow task into the arguments sent to a remote processing job.
For example, `SparkSubmitOperator` can be set up like this:

```python
SparkSubmitOperator(
    task_id="my_task",
    application="/script.py",
    conf={
      # separated components
      "spark.openlineage.parentJobNamespace": "{{ macros.OpenLineagePlugin.lineage_job_namespace() }}",
      "spark.openlineage.parentJobName": "{{ macros.OpenLineagePlugin.lineage_job_name(task_instance) }}",
      "spark.openlineage.parentRunId": "{{ macros.OpenLineagePlugin.lineage_run_id(task_instance) }}",
    },
    dag=dag,
)
```

Same information, but compacted to one string, can be passed using `linage_parent_id(task_instance)` macro:

```python
def my_task_function(templates_dict, **kwargs):
    parent_job_namespace, parent_job_name, parent_run_id = templates_dict["parentRunInfo"].split("/")
    ...


PythonOperator(
    task_id="render_template",
    python_callable=my_task_function,
    templates_dict={
        # joined components as one string `<namespace>/<name>/<run_id>`
        "parentRunInfo": "{{ macros.OpenLineagePlugin.lineage_parent_id(task_instance) }}",
    },
    provide_context=False,
    dag=dag,
)
```

## Secrets redaction
The integration uses Airflow SecretsMasker to hide secrets from produced metadata events. As not all fields in the metadata should be redacted, `RedactMixin` is used to pass information about which fields should be ignored by the process. 

Typically, you should subclass `RedactMixin` and use the `_skip_redact` attribute as a list of names of fields to be skipped.

However, all facets inheriting from `BaseFacet` should use the `_additional_skip_redact` attribute as an addition to the regular list (`['_producer', '_schemaURL']`).

## Development

To install all dependencies for _local_ development:

The Airflow integration depends on `openlineage.sql`, `openlineage.common` and `openlineage.client.python`. You should install them first independently or try to install them with following command:

```bash
$ pip install -r dev-requirements.txt
```

There is also a bash script that can run an arbitrary Airflow image with an OpenLineage integration built from the current branch. Additionally, it mounts OpenLineage Python packages as Docker volumes. This enables you to change your code without the need to constantly rebuild Docker images to run tests.
Run it as:
```bash
$ AIRFLOW_IMAGE=<airflow_image_with_tag> ./scripts/run-dev-airflow.sh [--help]
```
### Unit tests
To run the entire unit test suite, use the below command:
```bash
$ tox
```
or choose one of the environments, e.g.:
```bash
$ tox -e py-airflow214
```
You can also skip using `tox` and run `pytest` on your own dev environment.
### Integration tests
The integration tests require the use of _docker compose_. There are scripts prepared to make build images and run tests easier.

```bash
$ AIRFLOW_IMAGE=<name-of-airflow-image> ./tests/integration/docker/up.sh
```

```bash
$ AIRFLOW_IMAGE=apache/airflow:2.3.1-python3.8 ./tests/integration/docker/up.sh
```

When using `run-dev-airflow.sh`, you can add the `-i` flag or `--attach-integration` flag to run integration tests in a dev environment.
This can be helpful when you need to run arbitrary integration tests during development. For example, the following command run in the integration container...
```bash
python -m pytest test_integration.py::test_integration[great_expectations_validation-requests/great_expectations.json]
```
...runs a single test which you can repeat after changes in code.

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2024 contributors to the OpenLineage project

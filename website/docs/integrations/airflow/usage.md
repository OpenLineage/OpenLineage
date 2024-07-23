---
sidebar_position: 1
title: Using the Airflow Integration
---

:::caution
This page is about Airflow's external integration that works mainly for Airflow versions <2.7. 
[If you're using Airflow 2.7+, look at native Airflow OpenLineage provider documentation.](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/index.html)  <br /><br /> 

The ongoing development and enhancements will be focused on the `apache-airflow-providers-openlineage` package, 
while the `openlineage-airflow` will primarily be updated for bug fixes. See [all Airflow versions supported by this integration](older.md#supported-airflow-versions)
:::

#### PREREQUISITES

- [Python 3.8](https://www.python.org/downloads)
- [Airflow >= 2.1,<2.8](https://pypi.org/project/apache-airflow)

To use the OpenLineage Airflow integration, you'll need a running [Airflow instance](https://airflow.apache.org/docs/apache-airflow/stable/start.html). You'll also need an OpenLineage-compatible [backend](https://github.com/OpenLineage/OpenLineage#scope).

#### INSTALLATION

Before installing check [supported Airflow versions](older.md#supported-airflow-versions).

To download and install the latest `openlineage-airflow` library run: 

```
openlineage-airflow
```

You can also add `openlineage-airflow` to your `requirements.txt` for Airflow.  

To install from source, run:

```bash
$ python3 setup.py install
```

#### CONFIGURATION

Next, specify where you want OpenLineage to send events. 

We recommend configuring the client with an `openlineage.yml` file that tells the client how to connect to an OpenLineage backend.  
[See how to do it.](../../client/python.md#configuration)

The simplest option, limited to HTTP client, is to use the environment variables.
For example, to send OpenLineage events to a local instance of [Marquez](https://github.com/MarquezProject/marquez), use:

```bash
OPENLINEAGE_URL=http://localhost:5000
OPENLINEAGE_ENDPOINT=api/v1/lineage # This is the default value when this variable is not set, it can be omitted in this example
OPENLINEAGE_API_KEY=secret_token # This is only required if authentication headers are required, it can be omitted in this example
```

To set up an additional configuration, or to send events to targets other than an HTTP server (e.g., a Kafka topic), [configure a client.](../../client/python.md#configuration)

> **_NOTE:_** If you use a version of Airflow older than 2.3.0, [additional configuration is required](older.md#airflow-21---22).

##### Environment Variables

The following environment variables are available specifically for the Airflow integration, in addition to [Python client variables](../../client/python.md#environment-variables).

| Name                                    | Description                                                                                                                                | Example                                                        |
|-----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------|
| OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE | Set to `False` if you want source code of callables provided in PythonOperator or BashOperator `NOT` to be included in OpenLineage events. | False                                                          |
| OPENLINEAGE_EXTRACTORS                  | The optional list of extractors class (as semi-colon separated string) in case you need to use custom extractors.                          | full.path.to.ExtractorClass;full.path.to.AnotherExtractorClass |
| OPENLINEAGE_NAMESPACE                   | The optional namespace that the lineage data belongs to. If not specified, defaults to `default`.                                          | my_namespace                                                   |
| OPENLINEAGE_AIRFLOW_LOGGING             | Logging level of OpenLineage client in Airflow (the OPENLINEAGE_CLIENT_LOGGING variable from python client has no effect here).            | DEBUG                                                          |

For backwards compatibility, `openlineage-airflow` also supports configuration via
`MARQUEZ_NAMESPACE`, `MARQUEZ_URL` and `MARQUEZ_API_KEY` variables, instead of standard
`OPENLINEAGE_NAMESPACE`, `OPENLINEAGE_URL` and `OPENLINEAGE_API_KEY`. 
Variables with different prefix should not be mixed together.


#### USAGE

When enabled, the integration will:

* On TaskInstance **start**, collect metadata for each task.
* Collect task input / output metadata (source, schema, etc.).
* Collect task run-level metadata (execution time, state, parameters, etc.)
* On TaskInstance **complete**, also mark the task as complete in Marquez.

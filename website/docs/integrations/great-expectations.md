---
sidebar_position: 6
title: Great Expectations
---

Great Expectations is a robust data quality tool. It runs suites of checks, called expectations, over a defined dataset. This dataset can be a table in a database, or a Spark or Pandas dataframe. Expectations are run by checkpoints, which are configuration files that describe not just the expectations to use, but also any batching, runtime configurations, and, importantly, the action list of actions run after the expectation suite completes.

To learn more about Great Expectations, visit their [documentation site](https://docs.greatexpectations.io/docs/).

## How does Great Expectations work with OpenLineage?

Great Expecations integrates with OpenLineage through the action list in a checkpoint. An OpenLineage action can be specified, which is triggered when all expectations are run. Data from the checkpoint is sent to OpenLineage, which can then be viewed in Marquez or Datakin.

## Preparing a Great Expectations project for OpenLineage

First, we specify where we want Great Expectations to send OpenLineage events by setting the `OPENLINEAGE_URL` environment variable. For example, to send OpenLineage events to a local instance of Marquez, use:

```bash
OPENLINEAGE_URL=http://localhost:5000
```

If data is being sent to an endpoint with an API key, then that key must be supplied as well:

```bash
OPENLINEAGE_API_KEY=123456789
```

We can optionally specify a namespace where the lineage events will be stored. For example, to use the namespace "dev":

```bash
OPENLINEAGE_NAMESPACE=dev
```

With these environment variables set, we can add the OpenLineage action to the action list of the Great Expecations checkpoint.
Note: this must be done for *each* checkpoint.
Note: when using the `GreatExpectationsOperator>=0.2.0` in Airflow, there is a boolean parameter, defaulting to `True`, that will automatically create this action list item when it detects the OpenLineage environment specified in the previous step.


In a python checkpoint, this looks like:

```python
action_list = [
    {
        "name": "store_validation_result",
        "action": {"class_name": "StoreValidationResultAction"},
    },
    {
        "name": "store_evaluation_params",
        "action": {"class_name": "StoreEvaluationParametersAction"},
    },
    {
        "name": "update_data_docs",
        "action": {"class_name": "UpdateDataDocsAction", "site_names": []},
    },
    {
        "name": "open_lineage",
            "action": {
            "class_name": "OpenLineageValidationAction",
            "module_name": "openlineage.common.provider.great_expectations",
            "openlineage_host": os.getenv("OPENLINEAGE_URL"),
            "openlineage_apiKey": os.getenv("OPENLINEAGE_API_KEY"),
            "openlineage_namespace": oss.getenv("OPENLINEAGE_NAMESPACE"),
            "job_name": "openlineage_job",
        },
    }
]
```

And in yaml:

```yaml
name: openlineage
       action:
         class_name: OpenLineageValidationAction
         module_name: openlineage.common.provider.great_expectations
         openlineage_host: <HOST>
         openlineage_apiKey: <API_KEY>
         openlineage_namespace: <NAMESPACE_NAME> # Replace with your job namespace; we recommend a meaningful namespace like `dev` or `prod`, etc.
         job_name: validate_my_dataset
```

Then run your Great Expecations checkpoint with the CLI or your integration of choice.

## Feedback

What did you think of this guide? You can reach out to us on [slack](http://bit.ly/OpenLineageSlack) and leave us feedback!  

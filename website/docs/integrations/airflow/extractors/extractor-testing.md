---
sidebar_position: 2
title: Testing Custom Extractors
---

:::caution
This page is about Airflow's external integration that works mainly for Airflow versions <2.7. 
[If you're using Airflow 2.7+, look at native Airflow OpenLineage provider documentation.](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/index.html)  <br /><br /> 

The ongoing development and enhancements will be focused on the `apache-airflow-providers-openlineage` package, 
while the `openlineage-airflow` will primarily be updated for bug fixes. See [all Airflow versions supported by this integration](../older.md#supported-airflow-versions)
:::

OpenLineage comes with a variety of extractors for Airflow operators out of the box, but not every operator is covered. And if you are using a custom operator you or your team wrote, you'll certainly need to write a custom extractor. This guide will walk you through how to set up testing in a local dev environment, the most important data structures to write tests for, unit testing private functions, and some notes on troubleshooting.

We assume prior knowledge of writing custom extractors. For details on multiple ways to write extractors, check out the Astronomer blog on [extractors](https://www.astronomer.io/blog/3-ways-to-extract-data-lineage-from-airflow/#using-custom-extractors-for-airflow-operators). This post builds on [Pursuing Lineage from Airflow using Custom Extractors](https://openlineage.io/blog/extractors/), and it is recommended to read that post first. To learn more about how Operators and Extractors work together under the hood, check out this [guide](https://openlineage.io/blog/operators-and-extractors-technical-deep-dive/).

## Testing set-up

We’ll use the same extractor that we built in the blog post, the `RedshiftDataExtractor`. When testing an extractor, we want to verify a few different sets of assumptions. The first set of assumptions are about the `TaskMetadata` object being created, specifically verifying that the object is being built with the correct input and output datasets and relevant facets. This is done in OpenLineage via pytest, with appropriate mocking and patching for connections and objects. In the OpenLineage repository, extractor unit tests are found in under `[integration/airflow/tests](https://github.com/OpenLineage/OpenLineage/tree/main/integration/airflow/tests)`. For custom extractors, these tests should go under a `tests` directory at the top level of your project hierarchy.

![An Astro project directory structure, with extractors in an `extractors`/ folder under `include/`, and tests under a top-level `tests/` folder.](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/95581136-2c1e-496a-ba51-a9b70256e004/Untitled.png)

An Astro project directory structure, with extractors in an `extractors`/ folder under `include/`, and tests under a top-level `tests/` folder.

### Testing the TaskMetadata object

For the `RedshiftDataExtractor`, this core extract test is actually run on `extract_on_complete()`, as the `extract()` method is empty. We’ll walk through a test function to see how we can ensure the output dataset is being built as expected (full test code [here](https://github.com/OpenLineage/OpenLineage/blob/main/integration/airflow/tests/extractors/test_redshift_data_extractor.py))

```python
# First, we add patching to mock our connection to Redshift.
@mock.patch(
    "airflow.providers.amazon.aws.operators.redshift_data.RedshiftDataOperator.hook",
    new_callable=PropertyMock,
)
@mock.patch("botocore.client")
def test_extract_e2e(self, mock_client, mock_hook):
		# Mock the descriptions we can expect from a real call.
		mock_client.describe_statement.return_value = self.read_file_json(
        "tests/extractors/redshift_statement_details.json"
    )
    mock_client.describe_table.return_value = self.read_file_json(
        "tests/extractors/redshift_table_details.json"
    )
		# Finish setting mock objects' expected values.
    job_id = "test_id"
    mock_client.execute_statement.return_value = {"Id": job_id}
    mock_hook.return_value.conn = mock_client

		# Set the extractor and ensure that the extract() method is not returning anything, as expected.
    extractor = RedshiftDataExtractor(self.task)
    task_meta_extract = extractor.extract()
    assert task_meta_extract is None

		# Run an instance of RedshiftDataOperator with the predefined test values.
    self.ti.run()

		# Run extract_on_complete() with the task instance object.
    task_meta = extractor.extract_on_complete(self.ti)

		# Assert that the correct job_id was used in the client call.
    mock_client.describe_statement.assert_called_with(Id=job_id)

		# Assert there is a list of output datasets.
    assert task_meta.outputs
		# Assert there is only dataset in the list.
    assert len(task_meta.outputs) == 1
		# Assert the output dataset name is the same as the table created by the operator query.
    assert task_meta.outputs[0].name == "dev.public.fruit"
		# Assert the output dataset has a parsed schema.
    assert task_meta.outputs[0].facets["schema"].fields is not None
		# Assert the datasource is the correct Redshift URI.
    assert (
        task_meta.outputs[0].facets["dataSource"].name
        == f"redshift://{CLUSTER_IDENTIFIER}.{REGION_NAME}:5439"
    )
		# Assert the uri is None (as it already exists in dataSource).
    assert task_meta.outputs[0].facets["dataSource"].uri is None
		# Assert the schema fields match the number of fields of the table created by the operator query.
    assert len(task_meta.outputs[0].facets["schema"].fields) == 3
		# Assert the output statistics match the results of the operator query.
    assert (
        OutputStatisticsOutputDatasetFacet(
            rowCount=1,
            size=11,
        ) == task_meta.outputs[0].facets['stats']
    )
```

Most of the assertions above are straightforward, yet all are important in ensuring that no unexpected behavior occurs when building the metadata object. Testing each facet is important, as data or graphs in the UI can render incorrectly if the facets are wrong. For example, if the `task_meta.outputs[0].facets["dataSource"].name` is created incorrectly in the extractor, then the operator’s task will not show up in the lineage graph, creating a gap in pipeline observability.

### Testing private functions

Private functions with any complexity beyond returning a string should be unit tested as well. An example of this is the `_get_xcom_redshift_job_id()` private function in the `RedshiftDataExtractor`. The unit test is shown below:

```python
@mock.patch("airflow.models.TaskInstance.xcom_pull")
def test_get_xcom_redshift_job_id(self, mock_xcom_pull):
    self.extractor._get_xcom_redshift_job_id(self.ti)
    mock_xcom_pull.assert_called_once_with(task_ids=self.ti.task_id)
```

Unit tests do not have to be particularly complex, and in this instance the single assertion is enough to cover the expected behavior that the function was called only once.

### Troubleshooting

Even with unit tests, an extractor may still not be operating as expected. The easiest way to tell if data isn’t coming through correctly is if the UI elements are not showing up correctly in the Lineage tab.

When testing code locally, Marquez can be used to inspect the data being emitted—or ***not*** being emitted. Using Marquez will allow you to figure out if the error is being caused by the extractor or the API. If data is being emitted from the extractor as expected but isn’t making it to the UI, then the extractor is fine and an issue should be opened up in OpenLineage. However, if data is not being emitted properly, it is likely that more unit tests are needed to cover extractor behavior. Marquez can help you pinpoint which facets are not being formed properly so you know where to add test coverage.

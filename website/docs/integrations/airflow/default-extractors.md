---
sidebar_position: 5
title: Exposing Lineage in Airflow Operators
---

:::caution
This page is about Airflow's external integration that works mainly for Airflow versions \<2.7. 
[If you're using Airflow 2.7+, look at native Airflow OpenLineage provider documentation.](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/index.html)  <br /><br /> 

The ongoing development and enhancements will be focused on the `apache-airflow-providers-openlineage` package, 
while the `openlineage-airflow` will primarily be updated for bug fixes. See [all Airflow versions supported by this integration](older.md#supported-airflow-versions)
:::

OpenLineage 0.17.0+ makes adding lineage to your data pipelines easy through support of direct modification of Airflow operators. This means that custom operators—built in-house or forked from another project—can provide you and your team with lineage data without requiring modification of the OpenLineage project. The data will still go to your lineage backend of choice, most commonly using the `OPENLINEAGE_URL` environment variable.

Lineage extraction works a bit differently under the hood starting with OpenLineage 0.17.0. While extractors in the OpenLineage project have a getter method for operator names that they’re associated with, the default extractor looks for two specific methods in the operator itself and calls them directly if found. This means that implementation now consists of just two methods in your operator.

Those methods are `get_openlineage_facets_on_start()` and `get_openlineage_facets_on_complete()`, called when the operator is first scheduled to run and when the operator has finished execution respectively. Either, or both, of the methods may be implemented by the operator.

In the rest of this doc, you will see how to write these methods within an operator class called `DfToGcsOperator`. This operator moves a Dataframe from an arbitrary source table using a supplied Python callable to a specified path in GCS. Thorough understanding of the `__init__()` and `execute()` methods of the operator is not required, but an abbreviated version of each method is given below for context. The final two methods in the class are `get_openlineage_facets_on_start()` and `get_openlineage_facets_on_complete()`, which we will be implementing piece-by-piece in the rest of the doc. They are provided here in their entirety for completeness.

```python
from openlineage.airflow.extractors.base import OperatorLineage
from openlineage.client.facet import (
    DataSourceDatasetFacet,
    DocumentationJobFacet,
    OwnershipJobFacet,
    OwnershipJobFacetOwners,
    SchemaDatasetFacet,
    SchemaField,
)
from openlineage.client.run import Dataset


class DfToGcsOperator():
    def __init__(
        self,
        task_id,
        python_callable,
        data_source,
        bucket=None,
        table=None,
        security_group,
        pipeline_phase,
        col_types=None,
        check_cols=True,
        **kwargs,
    ):
        """Initialize a DfToGcsOperator."""
        super().__init__(task_id=task_id, **kwargs)
        self.python_callable = python_callable
        self.data_source = data_source
        self.table = table if table is not None else task_id
        self.bucket = bucket
        self.security_group = security_group
        self.pipeline_phase = pipeline_phase
        # col_types is a dict that stores expected column names and types, 
        self.col_types = col_types
        self.check_cols = check_cols

        self.base_path = "/".join(
            [self.security_group, self.pipeline_phase, self.data_source, self.table]
        )
        # Holds meta information about the dataframe, col names and col types,
        # that are used in the extractor.
        self.df_meta = None

    def execute(self, context):
        """
        Run a DfToGcs task.

        The task will run the python_callable and save
        the resulting dataframe to GCS under the proper object path
        <security_group>/<pipeline_phase>/<data_source>/<table>/.
        """
        ...
        
        df = get_python_callable_result(self.python_callable, context)
        if len(df) > 0:
            df.columns = [clean_column_name(c) for c in df.columns]
            if self.col_types and self.check_cols:
                check_cols = [c.lower().strip() for c in self.col_types.keys()]
                missing = [m for m in check_cols if m not in df.columns]
                assert (
                    len(missing) == 0
                ), "Columns present in col_types but not in DataFrame: " + ",".join(
                    missing
                )

            # ----------- #
            # Save to GCS #
            # ----------- #

            # Note: this is an imported helper function.
            df_to_gcs(df, self.bucket, save_to_path)

            # ----------- #
            # Return Data #
            # ----------- #

            # Allow us to extract additional lineage information
            # about all of the fields available in the dataframe
            self.df_meta = extract_df_fields(df)
        else:
            print("Empty dataframe, no artifact saved to GCS.")

    def extract_df_fields(df):
        from openlineage.common.dataset import SchemaField
        """Extract a list of SchemaFields from a DataFrame."""
        fields = []
        for (col, dtype) in zip(df.columns, df.dtypes):
            fields.append(SchemaField(name=col, type=str(dtype)))
        return fields

    def get_openlineage_facets_on_start(self):
        """Add lineage to DfToGcsOperator on task start."""
        if not self.bucket:
            ol_bucket = get_env_bucket()
        else:
            ol_bucket = self.bucket

        input_uri = "://".join([self.data_source, self.table])
        input_source = DataSourceDatasetFacet(
            name=self.table,
            uri=input_uri,
        )

        input_facet = {
            "datasource": input_source,
            "schema": SchemaDatasetFacet(
                fields=[
                    SchemaField(name=col_name, type=col_type)
                    for col_name, col_type in self.col_types.items()
                ]
            ),
        }

        input = Dataset(namespace=self.data_source, name=self.table, facets=input_facet)

        output_namespace = "gs://" + ol_bucket
        output_name = self.base_path
        output_uri = "/".join(
            [
                output_namespace,
                output_name,
            ]
        )

        output_source = DataSourceDatasetFacet(
            name=output_name,
            uri=output_uri,
        )

        output_facet = {
            "datasource": output_source,
            "schema": SchemaDatasetFacet(
                fields=[
                    SchemaField(name=col_name, type=col_type)
                    for col_name, col_type in self.col_types.items()
                ]
            ),
        }

        output = Dataset(
            namespace=output_namespace,
            name=output_name,
            facets=output_facet,
        )

        return OperatorLineage(
            inputs=[input],
            outputs=[output],
            run_facets={},
            job_facets={
                "documentation": DocumentationJobFacet(
                    description=f"""
                    Takes data from the data source {input_uri}
                    and puts it in GCS at the path: {output_uri}
                    """
                ),
                "ownership": OwnershipJobFacet(
                    owners=[OwnershipJobFacetOwners(name=self.owner, type=self.email)]
                ),
            }
        )

    def get_openlineage_facets_on_complete(self, task_instance):
        """Add lineage to DfToGcsOperator on task completion."""
        starting_facets = self.get_openlineage_facets_on_start()
        if task_instance.task.df_meta is not None:
            for i in starting_facets.inputs:
                i.facets["SchemaDatasetFacet"].fields = task_instance.task.df_meta
        else:
            starting_facets.run_facets = {
                "errorMessage": ErrorMessageRunFacet(
                    message="Empty dataframe, no artifact saved to GCS.",
                    programmingLanguage="python"
                )
            }
        return starting_facets
```

## Implementing lineage in an operator

Not surprisingly, you will need an operator class to implement lineage collection in an operator. Here, we’ll use the `DfToGcsOperator`, a custom operator created by the Astronomer Data team to load arbitrary dataframes to our GCS bucket. We’ll implement both `get_openlineage_facets_on_start()` and `get_openlineage_facets_on_complete()` for our custom operator. The specific details of the implementation will vary from operator to operator, but there will always be five basic steps that these functions will share.

Both the methods return an `OperatorLineage` object, which itself is a collection of facets. Four of the five steps mentioned above are creating these facets where necessary, and the fifth is creating the `DataSourceDatasetFacet`. First, though, we’ll need to import some OpenLineage objects:

```python
from openlineage.airflow.extractors.base import OperatorLineage
from openlineage.client.facet import (
    DataSourceDatasetFacet,
    SchemaDatasetFacet,
    SchemaField,
)
from openlineage.client.run import Dataset
```

Now, we’ll start building the facets for the `OperatorLineage` object in the `get_openlineage_facets_on_start()` method.

### 1. `DataSourceDatasetFacet`

The `DataSourceDatasestFacet` is a simple object, containing two fields, `name` and `uri`, which should be populated with the unique name of the data source and the URI. We’ll make two of these objects, an `input_source` to specify where the data came from and an `output_source` to specify where the data is going.

A quick note about the philosophy behind the `name` and `uri` in the OpenLineage spec: the `uri` is built from the `namespace` and the `name`, and each is expected to be unique with respect to its environment. This means a `namespace` should be globally unique in the OpenLineage universe, and the `name` unique within the `namespace`. The two are then concatenated to form the `uri`, so that `uri = namespace + name`. The full naming spec can be found [here](https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md).

In our case, the input `name` will be the table we are pulling data from, `self.table`, and the `namespace` will be our `self.data_source`.

```python
input_source = DataSourceDatasetFacet(
    name=self.table,
    uri="://".join([self.data_source, self.table]),
)
```

The output data source object’s `name` will always be the base path given to the operator, `self.base_path`. The `namespace` is always in GCS, so we use the OpenLineage spec’s `gs://` as the scheme and our bucket as the authority, giving us `gs://{ol_bucket}`. The `uri` is simply the concatenation of the two.

```python
if not self.bucket:
    ol_bucket = get_env_bucket()
else:
    ol_bucket = self.bucket

output_namespace = "gs://" + ol_bucket
output_name = self.base_path
output_uri = "/".join(
    [
        output_namespace,
        output_name,
    ]
)

output_source = DataSourceDatasetFacet(
    name=output_name,
    uri=output_uri,
)
```

### 2. Inputs

Next we’ll create the input dataset object. As we are moving data from a dataframe to GCS in this operator, we’ll make sure that we are capturing all the info in the dataframe being extracted in a `Dataset`. To create the `Dataset` object, we’ll need `namespace`, `name`, and `facets` objects. The first two are strings, and `facets` is a dictionary.

Our `namespace` will come from the operator, where we use `self.data_source` again. The `name` parameter for this facet will be the table, again coming from the operator’s parameter list. The `facets` will contain two entries, the first being our `DataSourceDatasetFacet` with the key "datasource" coming from the previous step and `input_source` being the value. The second has the key "schema", with the value being a `SchemaDatasetFacet`, which itself is a collection of `SchemaField` objects, one for each column, created via a list comprehension over the operator's `self.col_types` parameter.

The `inputs` parameter to `OperatorLineage` is a list of `Dataset` objects, so we’ll end up adding a single `Dataset` object to the list later. The creation of the `Dataset` object looks like the following:

```python
input_facet = {
    "datasource": input_source,
    "schema": SchemaDatasetFacet(
        fields=[
            SchemaField(name=col_name, type=col_type)
            for col_name, col_type in self.col_types.items()
        ]
    ),
}

input = Dataset(namespace=self.data_source, name=self.table, facets=input_facet)
```

### 3. Outputs

Our output facet will closely resemble the input facet, except it will use the `output_source` we previously created, and will also have a different `namespace`. Our output facet object will be built as follows:

```python
output_facet = {
    "datasource": output_source,
    "schema": SchemaDatasetFacet(
        fields=[
            SchemaField(name=col_name, type=col_type)
            for col_name, col_type in self.col_types.items()
        ]
    ),
}

output = Dataset(
    namespace=output_namespace,
    name=output_name,
    facets=output_facet,
)
```

### 4. Job facets

A Job in OpenLineage is a process definition that consumes and produces datasets. The Job evolves over time, and this change is captured when the Job runs. This means the facets we would want to capture in the Job level are independent of the state of the Job. Custom facets can be created to capture this Job data. For our operator, we went with pre-existing job facets, the `DocumentationJobFacet` and the `OwnershipJobFacet`:

```python
job_facets = {
    "documentation": DocumentationJobFacet(
        description=f"""
            Takes data from the data source {input_uri}
            and puts it in GCS at the path: {output_uri}
            """
    ),
    "ownership": OwnershipJobFacet(
        owners=[OwnershipJobFacetOwners(name=self.owner, type=self.email)]
    )
}
```

### 5. Run facets

A Run is an instance of a Job execution. For example, when an Airflow Operator begins execution, the Run state of the OpenLineage Job transitions to Start, then to Running. When writing an emitter, this means a Run facet should contain information pertinent to the specific instance of the Job, something that could change every Run.

In this example, we will output an error message when there is an empty dataframe, using the existing `ErrorMessageRunFacet`.

```python
starting_facets.run_facets = {
    "errorMessage": ErrorMessageRunFacet(
        message="Empty dataframe, no artifact saved to GCS.",
        programmingLanguage="python"
    )
}
```

### 6. On complete

Finally, we’ll implement the `get_openlineage_metadata_on_complete()` method. Most of our work has already been done for us, so we will start by calling `get_openlineage_metadata_on_start()` and then modifying the returned object slightly before returning it again. The two main additions here are replacing the original `SchemaDatasetFacet` fields and adding a potential error message to the `run_facets`.

For the `SchemaDatasetFacet` update, we replace the old fields facet with updated ones based on the now-filled-out `df_meta` dict, which is populated during the operator’s `execute()` method and is therefore unavailable to `get_openlineage_metadata_on_start()`. Because `df_meta` is already a list of `SchemaField` objects, we can set the property directly. Although we use a for loop here, the operator ensures only one dataframe will ever be extracted per execution, so the for loop will only ever run once and we therefore do not have to worry about multiple input dataframes updating.

The `run_facets` update is performed only if there is an error, which is a mutually exclusive event to updating the fields facets. We pass the same message to this facet that is printed in the `execute()` method when an empty dataframe is found. This error message does not halt operator execution, as it gets added *****after***** execution, but it does create an alert in the Marquez UI.

```python
def get_openlineage_facets_on_complete(self, task_instance):
    """Add lineage to DfToGcsOperator on task completion."""
    starting_facets = self.get_openlineage_facets_on_start()
    if task_instance.task.df_meta is not None:
        for i in starting_facets.inputs:
            i.facets["SchemaDatasetFacet"].fields = task_instance.task.df_meta
    else:
        starting_facets.run_facets = {
            "errorMessage": ErrorMessageRunFacet(
                message="Empty dataframe, no artifact saved to GCS.",
                programmingLanguage="python"
            )
        }
	return starting_facets
```

And with that final piece of the puzzle, we have a working implementation of lineage extraction from our custom operator!

### Custom Facets

The OpenLineage spec might not contain all the facets you need to write your extractor, in which case you will have to make your own [custom facets](https://openlineage.io/docs/spec/facets/custom-facets). More on creating custom facets can be found [here](https://openlineage.io/blog/extending-with-facets/).

### Testing

For information about testing your implementation, see the doc on [testing custom extractors](https://openlineage.io/docs/integrations/airflow/extractors/extractor-testing).

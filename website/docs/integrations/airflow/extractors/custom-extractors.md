---
sidebar_position: 1
title: Custom Extractors
---

:::caution
This page is about Airflow's external integration that works mainly for Airflow versions \<2.7. 
[If you're using Airflow 2.7+, look at native Airflow OpenLineage provider documentation.](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/index.html)  <br /><br /> 

The ongoing development and enhancements will be focused on the `apache-airflow-providers-openlineage` package, 
while the `openlineage-airflow` will primarily be updated for bug fixes. See [all Airflow versions supported by this integration](../older.md#supported-airflow-versions)
:::

This integration works by detecting which Airflow operators your DAG is using, and extracting lineage data from them using corresponding extractors.

However, not all operators are covered. In particular, third party providers may not be. To handle this situation, OpenLineage allows you to provide custom extractors for any operators where there is not one built-in.

If you want to extract lineage from your own Operators, you may prefer directly implementing [lineage support as described here](../default-extractors.md).


## Interface

Custom extractors have to derive from `BaseExtractor`.

Extractors have three methods to implement: `extract`, `extract_on_complete` and `get_operator_classnames`.
The last one is a classmethod that is used to provide list of operators that your extractor can get lineage from.

For example:

```python
@classmethod
def get_operator_classnames(cls) -> List[str]:
  return ['PostgresOperator']
```

If the name of the operator matches one of the names on the list, the extractor will be instantiated - with operator
provided in the extractor's `self.operator` property - and both `extract` and `extract_on_complete` methods will be called. 
They are used to provide actual information data. The difference is that `extract` is called before operator's `execute` 
method, while `extract_on_complete` is called after. This can be used to extract any additional information that the operator
sets on it's own properties. Good example is `SnowflakeOperator` that sets `query_ids` after execution.

Both methods return `TaskMetadata` structure:

```python
@attr.define
class TaskMetadata:
    name: str = attr.ib()  # deprecated
    inputs: List[Dataset] = attr.field(factory=list)
    outputs: List[Dataset] = attr.field(factory=list)
    run_facets: Dict[str, BaseFacet] = attr.field(factory=dict)
    job_facets: Dict[str, BaseFacet] = attr.field(factory=dict)
```

Inputs and outputs are lists of plain [OpenLineage datasets](../../../client/python.md) 

`run_facets` and `job_facets` are dictionaries of optional [JobFacets](../../../client/python.md) and [RunFacets](../../../client/python.md) that would be attached to the job - for example,
you might want to attach `SqlJobFacet` if your operator is executing SQL.

To learn more about facets in OpenLineage, please visit this [section](../../../spec/facets).


## Registering custom extractor

OpenLineage integration does not know that you've provided an extractor unless you'll register it.

The way to do that is to add them to `OPENLINEAGE_EXTRACTORS` environment variable.
```
OPENLINEAGE_EXTRACTORS=full.path.to.ExtractorClass
```

If you have multiple custom extractors, separate the paths with comma `(;)` 
```
OPENLINEAGE_EXTRACTORS=full.path.to.ExtractorClass;full.path.to.AnotherExtractorClass
```

Optionally, you can separate them with whitespace. It's useful if you're providing them as part of some YAML file.

```
OPENLINEAGE_EXTRACTORS: >-
  full.path.to.FirstExtractor;
  full.path.to.SecondExtractor
``` 

Remember to make sure that the path is importable for scheduler and worker.

## Adding extractor to OpenLineage Airflow integration package

All Openlineage extractors are defined in [this path](https://github.com/OpenLineage/OpenLineage/blob/main/integration/airflow/openlineage/airflow/extractors).
In order to add new extractor you should put your code in this directory. Additionally, you need to add the class to `_extractors` list in [extractors.py](https://github.com/OpenLineage/OpenLineage/blob/main/integration/airflow/openlineage/airflow/extractors/extractors.py), e.g.:

```python
_extractors = list(
    filter(
        lambda t: t is not None,
        [
            try_import_from_string(
                'openlineage.airflow.extractors.postgres_extractor.PostgresExtractor'
            ),
            ... # other extractors are listed here
+           try_import_from_string(
+               'openlineage.airflow.extractors.new_extractor.ExtractorClass'
+           ),
        ]
    )
)
```

## Debugging issues

There are two common problems associated with custom extractors. 
First, is wrong path provided to `OPENLINEAGE_EXTRACTORS`. 
The path needs to be exactly the same as one you'd use from your code. If the path is wrong or non-importable from worker, 
plugin will fail to load the extractors and proper OpenLineage events for that operator won't be emitted.

Second one, and maybe more insidious, are imports from Airflow. Due to the fact that OpenLineage code gets instantiated when
Airflow worker itself starts, any import from Airflow can be unnoticeably cyclical. This causes OpenLineage extraction to fail.

To avoid this issue, import from Airflow only locally - in `extract` or `extract_on_complete` methods. If you need imports for 
type checking, guard them behind `typing.TYPE_CHECKING`.

You can also check [Development section](../../../development/developing/) to learn more about how to setup development environment and create tests.
# Exposing lineage in Airflow operators

Since: 0.18.0

## Scope
The goal is to move the lineage extraction logic as close as possible to the operator logic. (for airflow operators in the airflow repository, for other providers in their own repository). 
This makes lineage extraction more stable as it lives with the operators. Previously the OpenLineage library required one `Extractor` for each supported `Operator` which is brittle and can break when operator internals changes. This also facilitates custom operator support.

Each operator is responsible for describing lineage per the spec below, but the actual lineage events are still being sent by the OpenLineage library in the TaskInstanceListener.

## Context
OpenLineage collects the following information regarding the Datasets being read and written by a task:

 - Dataset name and namespace [required] - the format for naming is outlined in the [naming specification](https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md#datasets).
 - Dataset schema [optional] - The column names and types, if known. Complex types, like structs and arrays are supported
 - Query id [optional] - for systems that expose an identifier, the id of the query (a Run facet, not a Dataset facet, but is always exposed by the Data Source’s proprietary API). For example operators for Bigquery, Redshift, and Snowflake should all allow this.
 - Input/output statistics [optional] - The number of records and/or bytes consumed or written.
    Example in the BigQuery extractor:
    - [Creating the relevant facet](https://github.com/OpenLineage/OpenLineage/blob/504f99e2f4dabd4f73a194dc5258ac81dae95d96/integration/common/openlineage/common/provider/bigquery.py#L111-L116).
    - [BigQuery API](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/get). [plan info](https://cloud.google.com/bigquery/docs/query-plan-explanation#query_plan_information)
 - Data quality metrics [optional] - Metrics associated with quality checks performed on the dataset. For example implemented by the Great Expectations integration.

Operators that intend to share information about the datasets being read and written should also expose either some of the above-mentioned information or some minimal information necessary to retrieve that information.

The absolute minimum information the operators need to share is 
 1. The type of datasource being accessed (e.g., bigquery, snowflake, postgres)
 2. The host or authority - this is often where the data is being hosted, such as the postgres server URL, the Hive metastore URL, the GCS bucket, the Snowflake account identifier...
 3. The fully qualified data path - this may be a table name, such as public.MyDataset.MyTable or a path in a bucket, e.g., path/to/my/data as defined in [the OpenLineage spec for consistency across operators](https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md).

This information needs to be shared for each dataset being read and written in a task. The naming spec in the OpenLineage repository uses the above information to construct a Dataset namespace and name, together they uniquely identify the dataset. 

For metadata about the execution of the task, a queryId or executionId should be exposed for data sources that support them. With that identifier, we can query the data source about the execution and gather statistics, such as "d" number of records read/written.

The astro library also includes data quality assertions, which we collect with OpenLineage and expose in the Datakin product. The OpenLineage [DataQuality facet specification can be found in here](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/DataQualityAssertionsDatasetFacet.json).

## Implementation
Each TaskInstance exposes the following methods returning the structure defined below:
 - get_openlineage_facets_on_start(ti)
 - get_openlineage_facets_on_complete(ti)
Facets are the json facets defined in the OpenLineage specification

```python
TaskInstanceLineage:
	runFacets: dict{}
	jobFacets: dict{}
inputs: [InputDataset]
outputs: [OutputDataset]
 
InputDataset:
	namespace: string
	name: string
	facets: dict{}
	inputFacets: dict{}

OutputDataset:
	namespace: string
	name: string
	facets: dict{}
	outputFacets: dict{}
```	
(all facets are optional)

When the task starts/completes, the OpenLineage TaskInstanceListener uses the selected method if available to construct lineage events. The order of selection of the method is as follows: if there is no extractor defined (based on get_operator_classnames it will fall back to DefaultExtractor. DefaultExtractor uses get_openlineage_facets_* methods. If the get_openlineage_facets_on_complete(ti) is not available it falls back to get_openlineage_facets_on_start(ti).

Example:

```json
{
 "runFacets": {
   "errorMessage": {
     "message": "could not connect to foo",
     "language": "python"
   }
 },
 "jobFacets": {
    "sql": {
      "query": "CREATE TABLE FOO AS SELECT * FROM BAR"
   }
 },
 "inputs": [{
   "namespace": "postgres://example",
   "name": "workshop.public.wealth",
   "facets": { 
     "schema": {
       "fields": [{ 
         "name": "foo"
         "type": "char"
         "description": "my first field"
         }, 
         ]
      }
   },
   "inputFacets": {
      "dataQualityMetrics": {
         "rowCount" : 1345
      }
   }
 }], 
 "outputs": [{
   "namespace": "postgres://example",
   "name": "workshop.public.death",
   "facets": {
      "schema": {
        "fields": [{ 
           "name": "foo"
           "type": "char"
           "description": "my first field"
        }, 
        ]
      }
   },
   "outputFacets": {
      "outputStatistics": {
         "rowCount": 10,
         "size": 1000
      }
   }
  }, {
   "namespace": "postgres://example",
   "name": "workshop.public.taxes"
   "facets": {
      "schema": {
        "fields": [{ 
           "name": "foo"
           "type": "char"
           "description": "my first field"
        }, 
        ]
      }
   },
   "outputFacets": {
      "outputStatistics": {
         "rowCount": 10,
         "size": 1000
      }
   }
 }], 
}
```
### Relevant facets
Here are some relevant examples of facets that can be added.
Please consult [the spec](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md#standard-facets) for the full list.
Custom facets can also be added, using a common facet name prefix.

#### Dataset facets
[Schema](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SchemaDatasetFacet.json)

```json
schema: {
	fields: [{ 
		Name: ””
		Type: ””
		Description: ””
}, …]
}
```

#### Output facets
[OutputStatistics](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/OutputStatisticsOutputDatasetFacet.json)

```json
outputStatistics: {
	rowCount: 10
	Size: 1000
}
```

#### Run facets
[ErrorMessage](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/ErrorMessageRunFacet.json)

```json
errorMessage: {
	Message: ””
	programmingLanguage: ””
	stackTrace: ””
}
```

#### All facets
[Facets](https://github.com/OpenLineage/OpenLineage/tree/main/spec/facets)






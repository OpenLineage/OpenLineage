---
sidebar_position: 6
---

# Job type Job Facet

Facet to contain job properties like: 
 * `processingType` which can be `STREAMING` or `BATCH`,
 * `integration` which can be `SPARK|DBT|AIRFLOW|FLINK`,
 * `jobType` which can be `QUERY|COMMAND|DAG|TASK|JOB|MODEL`.

Example:

```json
{
    ...
    "job": {
        "facets": {
            "jobType": {
                "processingType": "BATCH",
                "integration": "SPARK",
                "jobType": "QUERY",
                "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
                "_schemaURL": "https://openlineage.io/spec/facets/2-0-2/JobTypeJobFacet.json"
            }
        }
	...
}
```

The examples for specific integrations: 

 * Integration: `SPARK`
    * Processing type: `STREAM`|`BATCH`
    * Job type: `JOB`|`COMMAND`
 * Integration: `AIRFLOW`
    * Processing type: `BATCH`
    * Job type: `DAG`|`TASK`
 * Integration: `DBT`
    * ProcessingType: `BATCH`
    * JobType: `PROJECT`|`MODEL`
 * Integration: `FLINK`
    * Processing type: `STREAMING`|`BATCH`
    * Job type: `JOB`

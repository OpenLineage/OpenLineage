---
sidebar_position: 8
---

# Job Run Parameters Facet

The facet contains the input parameters and configurations passed to the Job at runtime. 

OpenLineage consumers can surface the documented parameters to provide users with more context about the job run. 

Example:

```json
{
    ...
    "run": {
        "facets": {
            "jobRunParameters": { 
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/JobRunParametersFacet.json",
                "parameters": [
                    {
                        "key": "airflow.dagRun.external_trigger",
                        "name": "External trigger", 
                        "description": "Determines whether the DAG run was triggered externally.",
                        "value": false
                    }
                ]
            }
        }
    }
    ...
}
```


The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/JobRunParametersFacet.json)
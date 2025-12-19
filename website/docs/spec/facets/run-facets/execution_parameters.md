---
sidebar_position: 8
---

# Execution Parameters Facet

The facet contains the input parameters and configurations provided to the Job either as input or during runtime. 

If you want to attach custom key-value tags so downstream tools can filter, group, or enrich lineage, use [Tags Run Facet](https://openlineage.io/docs/spec/facets/run-facets/tag-facet) instead.

OpenLineage consumers can surface the documented parameters to provide users with more context about the job run. 

Example:

```json
{
    ...
    "run": {
        "facets": {
            "executionParameters": { 
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/ExecutionParametersRunFacet.json",
                "parameters": [
                    {
                        "key": "task.timeout",
                        "name": "Task timeout", 
                        "description": "How long (in seconds) can individual taks run without timing out.",
                        "value": 3600
                    }
                ]
            }
        }
    }
    ...
}
```


The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/ExecutionParametersRunFacet.json)
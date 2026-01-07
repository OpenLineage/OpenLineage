---
sidebar_position: 9
---

# Tags Run Facet

The facet contains the tags associated with the run. Use to attach custom key-value tags so downstream tools can filter, group, or enrich lineage.

If you want to capture input parameters and configurations provided to the Job either as input or during runtime, use [Execution Parameters Facet](https://openlineage.io/docs/spec/facets/run-facets/execution_parameters) instead.

Example:

```json
{
    ...
    "job": {
        "facets": {
            "_producer": "https://some.producer.com/version/1.0",
            "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/TagsJobFacet.json",
            "tags": [{
                "key": "containerId",
                "value": "08047900167b20192704669334768182f825281777f540",
                "source": "RUNTIME" 
            }, {
                "key": "clusterId",
                "value": "staging-cluster-01",
                "source": "RUNTIME"
            }]
        }
    }
    ...
}
```


The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/TagsRunFacet.json)
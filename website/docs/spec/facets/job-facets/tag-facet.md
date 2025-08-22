---
sidebar_position: 7
---

# Tags Job Facet

The facet contains the tags associated with the job.

Example:

```json
{
    ...
    "job": {
        "facets": {
            "_producer": "https://some.producer.com/version/1.0",
            "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/TagsJobFacet.json",
            "tags": [{
                "key": "environment",
                "value": "production",
                "source": "CONFIG" 
            }, {
                "key": "team",
                "value": "data-engineering",
                "source": "CONFIG"
            }]
        }
    }
    ...
}
```


The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/TagsJobFacet.json)
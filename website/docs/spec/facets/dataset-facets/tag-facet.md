---
sidebar_position: 10
---

# Tags Dataset Facet

The facet contains the tags associated with the dataset.

Example:

```json
{
    ...
    "job": {
        "facets": {
            "_producer": "https://some.producer.com/version/1.0",
            "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/TagsDatasetFacet.json",
            "tags": [{
                "key": "environment",
                "value": "production",
                "source": "CONFIG" 
            }, {
                "key": "classification",
                "value": "PII",
                "source": "CONFIG",
                "field": "tax_id"
            }]
        }
    }
    ...
}
```


The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/TagsDatasetFacet.json)
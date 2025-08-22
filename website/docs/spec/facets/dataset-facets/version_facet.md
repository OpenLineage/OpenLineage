---
sidebar_position: 12
---

# Version Facet

Example:

```json
{
    ...
    "inputs": {
        "facets": {
            "version": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasetVersionDatasetFacet.json",
                "datasetVersion": "1"
            }
        }
    }
    ...
}
```
The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/DatasetVersionDatasetFacet.json).
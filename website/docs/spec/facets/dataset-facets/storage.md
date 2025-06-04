---
sidebar_position: 8
---

# Storage Facet

Example:

```json
{
    ...
    "inputs": {
        "facets": {
            "storage": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/StorageDatasetFacet.json",
                "storageLayer": "iceberg",
                "fileFormat": "csv"
            }
        }
    }
    ...
}
```
The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/StorageDatasetFacet.json).
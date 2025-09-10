---
sidebar_position: 13
---

# Dataset Type Facet

The facet contains type of dataset within a database.

Fields description:
- `datasetType`: Dataset type, e.g. `TABLE`, `VIEW`, `TOPIC`, `MODEL`.
- `subType`: sub-type within `datasetType`, e.g. `MATERIALIZED`, `EXTERNAL`.

Example:

```json
{
    ...
    "inputs": {
        "facets": {
            "datasetType": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasetTypeDatasetFacet.json",
                "datasetType": "VIEW",
                "subType": "MATERIALIZED"
            }
        }
    }
    ...
}
```
The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/DatasetTypeDatasetFacet.json).

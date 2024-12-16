---
sidebar_position: 1
---

# Input Statistics Facet

Example:

```json
{
    ...
    "inputs": {
        "inputFacets": {
            "inputStatistics": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/InputStatisticsInputDatasetFacet.json",
                "rowCount": 123,
                "fileCount": 5,
                "size": 35602
            }
        }
    }
    ...
}
```
The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/InputStatisticsInputDatasetFacet.json).

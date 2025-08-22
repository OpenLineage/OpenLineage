---
sidebar_position: 5
---

# Datasource Facet

Example:

```json
{
    ...
    "inputs": {
        "facets": {
            "dataSource": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json",
                "name": "datasource_one",
                "url": "https://some.location.com/datsource/one"
            }
        }
    }
    ...
}
```
The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json).
---
sidebar_position: 8
---

# Symlinks Facet

Example:

```json
{
    ...
    "inputs": {
        "facets": {
            "symlinks": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SymlinksDatasetFacet.json",
                "identifiers": [
                    "namespace": "example_namespace",
                    "name": "example_dataset_1",
                    "type": "table"
                ]
            }
        }
    }
    ...
}
```
The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/SymlinksDatasetFacet.json).
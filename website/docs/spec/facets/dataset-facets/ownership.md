---
sidebar_position: 8
---

# Ownership Dataset Facet

Example:

```json
{
    ...
    "inputs": {
        "facets": {
            "ownership": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/OwnershipDatasetFacet.json",
                "owners": [
                    {
                        "name": "maintainer_one",
                        "type": "MAINTAINER"
                    }
                ]
            }
        }
    }
    ...
}
```

The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/OwnershipDatasetFacet.json).
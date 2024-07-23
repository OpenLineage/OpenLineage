---
sidebar_position: 2
---

# Ownership Job Facet


The facet that contains the information regarding users or group who owns this particular job.

Example:

```json
{
    ...
    "job": {
        "facets": {
            "ownership": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/OwnershipJobFacet.json",
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


The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/OwnershipJobFacet.json)
---
sidebar_position: 3
---


# External Query Facet

The facet that describes the identification of the query that the run is related to which was executed by external systems. Even though the query itself is not contained, using this facet, the user should be able to access the query and its details.

Example:

```json
{
    ...
    "run": {
        "facets": {
            "externalQuery": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/ExternalQueryRunFacet.json",
                "externalQueryId": "my-project-1234:US.bquijob_123x456_123y123z123c",
                "source": "bigquery"
            }
        }
    }
    ...
}
```


The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/ExternalQueryRunFacet.json)
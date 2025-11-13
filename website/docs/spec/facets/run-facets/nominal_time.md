---
sidebar_position: 6
---


# Nominal Time Facet

The facet to describe the nominal start and end time of the run. The nominal usually means the time the job run was expected to run (like a scheduled time), and the actual time can be different.

Example:

```json
{
    ...
    "run": {
        "facets": {
            "nominalTime": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SQLJobFacet.json",
                "nominalStartTime": "2020-12-17T03:00:00.000Z",
                "nominalEndTime": "2020-12-17T03:05:00.000Z"
            }
        }
    }
    ...
}
```


The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/NominalTimeRunFacet.json)
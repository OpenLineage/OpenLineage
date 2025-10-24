---
sidebar_position: 13
---

# Dataset Type Facet

The facet contains type of dataset within a data source.

Fields description:
- `datasetType`: Dataset type, e.g. `TABLE`, `VIEW`, `FILE`, `TOPIC`, `STREAM`, `MODEL`, `JOB_OUTPUT`.
- `subType`: sub-type within `datasetType`, e.g. `MATERIALIZED`, `EXTERNAL`, `TEMPORARY`.

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

Recommended values for specific edge cases: 
- use `datasetType:= JOB_OUTPUT` and `subType:= TEMPORARY` to represent temporary (artificial) datasets when documenting job-to-job lineage. Consumers of the OpenLineage event can choose not to represent them (and draw the lineage directly to the next task instead). 

The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/DatasetTypeDatasetFacet.json).

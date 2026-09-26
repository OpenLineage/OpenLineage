---
sidebar_position: 15
---

# Lineage Dataset Facet

The Lineage Dataset Facet describes the exact datasets, jobs, and fields from
which a dataset derives. Put it at `dataset.facets.lineage` on a DatasetEvent.
The target is the dataset that carries the facet, so its namespace and name are
not repeated inside the facet.

This form is useful for structural relationships that have no natural event
job, such as a view derived from tables, an alias, or lineage curated in a data
catalog.

```json
{
  "dataset": {
    "namespace": "postgresql://warehouse",
    "name": "analytics.active_customers",
    "facets": {
      "lineage": {
        "_producer": "https://example.com/catalog",
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/LineageFacet.json#/$defs/LineageDatasetFacet",
        "inputs": [
          {
            "namespace": "postgresql://warehouse",
            "name": "raw.customers",
            "type": "DATASET"
          }
        ],
        "fields": {
          "customer_id": {
            "inputs": [
              {
                "namespace": "postgresql://warehouse",
                "name": "raw.customers",
                "type": "DATASET",
                "field": "id",
                "transformations": [
                  {
                    "type": "DIRECT",
                    "subtype": "IDENTITY"
                  }
                ]
              }
            ]
          }
        }
      }
    }
  }
}
```

Use `inputs` for entity-level dependencies and `fields` for field-level
dependencies. A source may be a dataset or a job. Producers should supply at
least one of `inputs` or `fields` so the facet communicates a relationship.

The Lineage Dataset Facet supersedes the Column Lineage Dataset Facet for the
relationships it describes. If both are present for the same dataset, consumers
should use the lineage facet.

The [facet specification](https://openlineage.io/spec/facets/1-0-0/LineageFacet.json#/$defs/LineageDatasetFacet)
contains the complete field definitions.

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2026 contributors to the OpenLineage project

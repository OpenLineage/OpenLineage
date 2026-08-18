---
sidebar_position: 8
---

# Lineage Job Facet

The Lineage Job Facet records explicit data-flow edges owned by a job. Put it at
`job.facets.lineage` on a RunEvent for lineage observed during one run, or on a
JobEvent for the job's declared lineage.

Each entry names one target dataset or job and lists only the sources that feed
that target. This avoids the false edges produced by treating every event input
as a source for every event output. For example, a job that reads `customers`
and `orders` but independently writes `customer_summary` and `order_summary`
can represent the two real edges without implying a four-edge Cartesian
product.

```json
{
  "job": {
    "namespace": "https://example.com/jobs",
    "name": "build_summaries",
    "facets": {
      "lineage": {
        "_producer": "https://example.com/lineage",
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/LineageFacet.json#/$defs/LineageJobFacet",
        "entries": [
          {
            "namespace": "postgresql://warehouse",
            "name": "analytics.customer_summary",
            "type": "DATASET",
            "inputs": [
              {
                "namespace": "postgresql://warehouse",
                "name": "raw.customers",
                "type": "DATASET"
              }
            ]
          },
          {
            "namespace": "postgresql://warehouse",
            "name": "analytics.order_summary",
            "type": "DATASET",
            "inputs": [
              {
                "namespace": "postgresql://warehouse",
                "name": "raw.orders",
                "type": "DATASET"
              }
            ]
          }
        ]
      }
    }
  }
}
```

Dataset targets may also contain a `fields` map for field-level lineage. Job
targets and inputs make job-to-job relationships possible when there is no
tracked dataset between the jobs. Omitting `namespace` and `name` from a
job-typed target or input refers to the event's own job; producers should
provide both identity fields or neither.

The event's `inputs` and `outputs` remain useful for carrying dataset facets,
supporting older consumers, and describing the implicit boundary of the event's
own job. When the lineage facet is present, consumers should not infer an
additional Cartesian product from those arrays. If the facet and a Column
Lineage Dataset Facet describe the same output, the lineage facet takes
precedence.

The [facet specification](https://openlineage.io/spec/facets/1-0-0/LineageFacet.json#/$defs/LineageJobFacet)
contains the complete field definitions.

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2026 contributors to the OpenLineage project

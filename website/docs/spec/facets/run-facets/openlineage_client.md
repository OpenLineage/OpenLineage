---
sidebar_position: 6
---

# OpenLineage Client Run Facet
The OpenLineage Client Run Facet provides detailed information about the OpenLineage client that emitted the event.

| Property                  | Description                                                                                          | Type   | Example  | Required |
|---------------------------|------------------------------------------------------------------------------------------------------|--------|----------|----------|
| version                   | The version of the OpenLineage client package that emitted the event, such as python or java client. | string | "1.38.0" | Yes      |

Example use case: When a data pipeline job fails, the OpenLineage Client Run Facet can be used to quickly identify the version of OpenLineage client that was used underneath by the integration like Airflow, Dbt or Spark, making it easier to replicate the issue and find a solution.

The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/OpenlineageClientRunFacet.json#/$defs/OpenlineageClientRunFacet).
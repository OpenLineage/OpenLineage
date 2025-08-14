---
sidebar_position: 8
---

# Processing Engine Run Facet
The Processing Engine Run Facet provides detailed information about the processing engine that executed the job. This facet is commonly used to track and document the specific engine and its version, ensuring reproducibility and aiding in debugging processes.

| Property                  | Description                                                                 | Type   | Example   | Required |
|---------------------------|-----------------------------------------------------------------------------|--------|-----------|----------|
| version                   | The version of the processing engine, such as Airflow or Spark. This helps in identifying the exact environment in which the job was run. | string | "2.5.0"   | Yes      |
| name                      | The name of the processing engine, for example, Airflow or Spark. This is useful for categorizing and filtering jobs based on the engine used. | string | "Airflow" | Yes      |
| openlineageAdapterVersion | The version of the OpenLineage adapter package used, such as the OpenLineage Airflow integration package version. This can be helpful for troubleshooting and ensuring compatibility. | string | "0.19.0" | No       |

Example use case: When a data pipeline job fails, the Processing Engine Run Facet can be used to quickly identify the version and type of processing engine that was used, making it easier to replicate the issue and find a solution.

The facet specification can be found [here](https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json#/$defs/ProcessingEngineRunFacet).
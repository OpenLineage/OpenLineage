---
sidebar_position: 1
---

# Catalog Dataset Facet

The facet contains information about the catalog that the processing engine
used when accessing this dataset.

Fields description:
- `framework`: The storage framework for which the catalog is configured (e.g., iceberg, delta, hive).
- `type`: Type of the catalog (e.g., jdbc, glue, polaris).
- `name`: Name of the catalog, as configured in the source system (e.g., my_iceberg_catalog).
- `metadataUri`: URI or connection string to the catalog, if applicable (e.g., jdbc:mysql://host:3306/iceberg_database).
- `warehouseUri`: URI or connection string to the physical location of the data that the catalog describes (e.g., s3://bucket/path/to/iceberg/warehouse).
- `source`: Source system where the catalog is configured (e.g., spark, flink, hive).

`framework`, `type` and `name` are required fields

Example:

```json
{
    ...
    "inputs": {
        "facets": {
            "catalog": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/CatalogDatasetFacet.json",
                "framework": "iceberg",
                "type": "polaris",
                "name": "my_iceberg_catalog",
                "metadataUri": "http://host:1234/iceberg_database",
                "warehouseUri": "s3://bucket/path/to/iceberg/warehouse",
                "source": "spark"
            }
        }
    }
    ...
}
```


The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/CatalogDatasetFacet.json)

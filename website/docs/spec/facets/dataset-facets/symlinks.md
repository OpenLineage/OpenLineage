---
sidebar_position: 11
---

# Symlinks Facet

The symlinks facet is used to list alternative identifiers for a single dataset. A dataset might be referenced by its physical location (e.g., a file path) in one context and by a logical name (e.g., a database table name) in another. This facet allows OpenLineage to understand that these different identifiers refer to the same entity, creating a unified lineage graph.

Fields Description
- `identifiers`: An array containing one or more alternative identifiers for the dataset.
    - `namespace`: The namespace of the alternative identifier (e.g., Glue Catalog).
    - `name`: The name of the dataset within the given namespace (e.g., Glue Table).
    - `type`: A string describing the type of the identifier.

`namespace`, `name` and `type` are required fields

Example:

```json
{
    ...
    "inputs": {
        "namespace": "s3://{bucket name}",
        "name": "{object key}",
        "facets": {
            "symlinks": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json",
                "identifiers": [
                    "namespace": "arn:aws:glue:{region}:{account id}",
                    "name": "table/{database name}/{table name}",
                    "type": "TABLE"
                ]
            }
        }
    }
    ...
}
```
The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json).

---
sidebar_position: 12
---

# Database Hierarchy Dataset Facet

The facet contains information about the hierarchy of objects within database.

Fields description:
- `hierarchy`: Database hierarchy levels (e.g. `DATABASE` -> `SCHEMA` -> `TABLE`), from highest to lowest level.
  Array of objects, the order is important:
  - `type`: Hierarchy level type (e.g. `DATABASE`, `SCHEMA`, `TABLE`).
  - `name`: Hierarchy level object name (e.g. `my_db`, `my_schema`, `my_table`).

Example:

```json
{
    ...
    "inputs": {
        "namespace": "...",
        "name": "...",
        "facets": {
          "databaseHierarchy": {
            "_producer": "https://some.producer.com/version/1.0",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatabaseHierarchyDatasetFacet.json#/$defs/DatabaseHierarchyDatasetFacet",
            "hierarchy": [
              {
                "type": "DATABASE",
                "name": "my_db"
              },
              {
                "type": "SCHEMA",
                "name": "my_schema"
              },
              {
                "type": "TABLE",
                "name": "my_table"
              }
            ]
          }
        }
    }
    ...
}
```


The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/DatabaseHierarchyDatasetFacet.json)
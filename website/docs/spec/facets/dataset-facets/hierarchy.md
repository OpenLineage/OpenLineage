---
sidebar_position: 12
---

# Hierarchy Dataset Facet

The facet contains information about the hierarchy of objects dataset belongs to (in database, filesystem or other storage).

Fields description:
- `hierarchy`: Hierarchy levels (e.g. `DATABASE` -> `SCHEMA` -> `TABLE`), from highest to lowest level.
  Array of objects, the order is important:
  - `type`: Hierarchy level type (e.g. `DATABASE`, `SCHEMA`, `TABLE`, etc).
  - `name`: Hierarchy level object name (e.g. `my_db`, `my_schema`, `my_table`).

Example:

```json
{
    ...
    "inputs": [{
        "namespace": "postgres://some.host:5432",
        "name": "my_db.my_schema.my_table",
        "facets": {
          "hierarchy": {
            "_producer": "https://some.producer.com/version/1.0",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/HierarchyDatasetFacet.json#/$defs/HierarchyDatasetFacet",
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
    }]
    ...
}
```


The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/HierarchyDatasetFacet.json)

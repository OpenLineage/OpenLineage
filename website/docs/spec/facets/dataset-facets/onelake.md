---
sidebar_position: 16
---

# OneLake Dataset Facet

Describes where a table is stored in OneLake, Microsoft Fabric's storage. It links a dataset to the Fabric
workspace and item, such as a lakehouse or a warehouse, that hold it, however the job reached the table: through a
warehouse's SQL address, a OneLake path written with IDs or with names, or a Spark catalog table.

All fields are optional. Producers set only what they know locally, without calling Fabric:

- `location`: where the table's files are, using IDs only:
  `abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{itemId}/Tables/{schema}/{table}`, or
  `.../{itemId}/Tables/{table}` in a lakehouse without schemas. IDs are lowercase, with no trailing slash. OneLake
  refuses paths that mix IDs and names, so this is set only when both IDs are known.
- `workspaceId`: ID of the Fabric workspace, in lowercase.
- `workspaceName`: name of the Fabric workspace.
- `itemId`: ID of the item that holds the table, in lowercase.
- `itemName`: name of the item that holds the table.
- `itemType`: type of the item, as Fabric names item types, such as `Lakehouse` or `Warehouse`. Set only when the
  producer knows it; for example, a Spark catalog can't tell a lakehouse from a warehouse.
- `tableId`: ID of the table's Delta metadata (`metaData.id` in the Delta log), in lowercase. For a table reached
  through a shortcut, this is the ID of the target table, so it links the shortcut to the table it points to.

Example:

```json
{
    ...
    "inputs": [{
        "namespace": "fabric-warehouse://6obaxtbopqfublwdicr5oicqlm-ac4ezc2eudhehd3wgnlpocnuna.datawarehouse.fabric.microsoft.com",
        "name": "wh_primary.raw.orders",
        "facets": {
            "oneLake": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/OneLakeDatasetFacet.json#/$defs/OneLakeDatasetFacet",
                "location": "abfss://8b4cb800-a044-43ce-8f76-3356f709b468@onelake.dfs.fabric.microsoft.com/0fd9a0c2-ee93-46af-aaea-a45c8649ed9d/Tables/raw/orders",
                "workspaceId": "8b4cb800-a044-43ce-8f76-3356f709b468",
                "workspaceName": "sales-analytics",
                "itemId": "0fd9a0c2-ee93-46af-aaea-a45c8649ed9d",
                "itemName": "wh_primary",
                "itemType": "Warehouse"
            }
        }
    }]
    ...
}
```

The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/OneLakeDatasetFacet.json).

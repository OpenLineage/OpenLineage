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
        "namespace": "fabric-warehouse://ck32i2hmt2qeziwtnklnft2ptq-hnd7nul2t5jehfalvdluiyot7q.datawarehouse.fabric.microsoft.com",
        "name": "wh_primary.raw.orders",
        "facets": {
            "oneLake": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/OneLakeDatasetFacet.json#/$defs/OneLakeDatasetFacet",
                "location": "abfss://d1f6473b-9f7a-4352-940b-a8d74461d3fc@onelake.dfs.fabric.microsoft.com/d75873bd-716b-4378-a37e-864c76fc9b3b/Tables/raw/orders",
                "workspaceId": "d1f6473b-9f7a-4352-940b-a8d74461d3fc",
                "workspaceName": "sales-analytics",
                "itemId": "d75873bd-716b-4378-a37e-864c76fc9b3b",
                "itemName": "wh_primary",
                "itemType": "Warehouse"
            }
        }
    }]
    ...
}
```

The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/OneLakeDatasetFacet.json).

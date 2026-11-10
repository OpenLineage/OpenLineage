---
sidebar_position: 9
title: Microsoft Fabric Warehouse
---

When you read or write a Fabric warehouse table with Microsoft's Spark connector (`synapsesql`), OpenLineage reports
the table itself, named as the [naming conventions](../../spec/naming.md) describe:

| Namespace                                                                           | Name                           |
|:------------------------------------------------------------------------------------|:-------------------------------|
| `fabric-warehouse://{workspace SQL address}`, such as `fabric-warehouse://ck32i…-hnd7n….datawarehouse.fabric.microsoft.com` | `{warehouse}.{schema}.{table}` |

For example, `spark.read.synapsesql("wh_primary.raw.orders")` is reported as reading
`fabric-warehouse://ck32i2hmt2qeziwtnklnft2ptq-hnd7nul2t5jehfalvdluiyot7q.datawarehouse.fabric.microsoft.com` /
`wh_primary.raw.orders`.

Each Fabric workspace has one SQL address, shared by all its warehouses.

:::note
Fabric comes with its own copy of the OpenLineage Spark integration, which doesn't do this. To get this behavior, load
the `openlineage-spark` jar yourself with `spark.jars`, and set `spark.driver.userClassPathFirst=true` so that your
copy is used instead of Fabric's.
:::

## Reads

The connector reads tables through an ordinary SQL Server connection to the workspace's SQL address. OpenLineage
recognizes Fabric addresses and reports these tables as `fabric-warehouse://`, not `sqlserver://`.

The same address can be written in several ways, so OpenLineage always writes it the same way: in lowercase, without
the `.z{xx}` label that Fabric sometimes adds (for example `.z8b`), and without port 1433, the default. This way a
table always gets the same name, however the connector wrote the address.

Addresses for a workspace in another organization (guest access) have an extra part and aren't recognized. Those
tables keep the `sqlserver://` name.

## Writes

The connector writes a table in two steps:

1. Spark saves the data as Parquet files in a temporary folder.
2. The connector loads those files into the table with `COPY INTO`. This step runs outside Spark.

OpenLineage only sees the first step. It reports the table being loaded as the output, not the temporary folder.

The first step doesn't include the workspace's SQL address, so OpenLineage works it out in this order:

1. If you set the address in `spark.datawarehouse.{warehouse}.sqlendpoint`, that address is used, as long as it is the
   address of a Fabric workspace. If it isn't, for example because it is an address for a workspace in another
   organization, the write is reported without an output.
2. Otherwise, the address is built from two IDs: the tenant ID, which Fabric provides in `trident.tenant.id`, and the
   workspace ID. The workspace ID comes from the connector's `WorkspaceId` option, or from `trident.workspace.id` if
   you don't set the option. When you write to a warehouse in a different workspace from your notebook, set
   `WorkspaceId`; otherwise the write is reported against the notebook's workspace.

If neither works, the write is reported without an output.

### How the address is built

The address is `{tenant part}-{workspace part}.datawarehouse.fabric.microsoft.com`. Each part is 26 characters made
from an ID: the ID's 16 bytes, in the order Microsoft's .NET stores them, written in lowercase base32 without `=`
padding.

Microsoft doesn't document this. It matches the addresses of real workspaces, and OpenLineage's tests check it against
known pairs of IDs and addresses. If Microsoft ever changes it, set the address yourself with
`spark.datawarehouse.{warehouse}.sqlendpoint`, as described above, to keep the names correct.

### Facets on written tables

Two facets differ from other file writes:

- **No `lifecycleStateChange`.** The temporary folder is always overwritten, whichever save mode you choose
  (append, overwrite and so on), and the mode you chose isn't visible to OpenLineage. Rather than report a possibly
  wrong mode, OpenLineage leaves this facet out.
- **`outputStatistics` describes the temporary files.** The row count matches the rows loaded into the table, but the
  byte size and file count are those of the temporary Parquet files.

## OneLake file location

If you pass the warehouse's ID with the connector's `DatawarehouseId` option, on a read or a write, OpenLineage also
reports where the table's files are stored in OneLake, as a `LOCATION` symlink:

```python
from com.microsoft.spark.fabric.Constants import Constants

df = (
    spark.read
    .option(Constants.WorkspaceId, "d1f6473b-9f7a-4352-940b-a8d74461d3fc")
    .option(Constants.DatawarehouseId, "d75873bd-716b-4378-a37e-864c76fc9b3b")
    .synapsesql("wh_primary.raw.orders")
)
```

This read gets the symlink `abfss://d1f6473b-9f7a-4352-940b-a8d74461d3fc@onelake.dfs.fabric.microsoft.com` /
`d75873bd-716b-4378-a37e-864c76fc9b3b/Tables/raw/orders`.

The location always uses IDs, because OneLake refuses paths that mix IDs and names. Without `DatawarehouseId`, the
connector doesn't make the warehouse's ID available, so no location is reported. The location is in the workspace of
the SQL address that was queried; if a `WorkspaceId` option names another workspace, no location is reported.

## The `fabric_onelake` facet

Every Fabric warehouse table that OpenLineage reports, read or written, also gets the `fabric_onelake` dataset facet
([schema](https://openlineage.io/spec/facets/1-0-0/FabricOneLakeDatasetFacet.json)). It describes where the table is
in OneLake, so you can link it to the warehouse that holds it, whichever name a job used for the table. Every field is
optional; producers set only what they know without calling Fabric. For warehouse tables, Spark sets:

| Field           | Value                                                                                                   |
|:----------------|:--------------------------------------------------------------------------------------------------------|
| `itemType`      | `Warehouse`                                                                                             |
| `itemName`      | the warehouse, the first part of the table's name                                                       |
| `workspaceId`   | the workspace ID, read from the SQL address                                                             |
| `workspaceName` | the workspace's name, from Fabric's `trident.workspace.name`, only for the workspace the notebook runs in |
| `itemId`        | the warehouse ID, only when the `LOCATION` symlink is reported (see above)                              |
| `location`      | the same OneLake location as the `LOCATION` symlink, as a single path                                   |

For the read above, the facet is:

```json
"fabric_onelake": {
  "location": "abfss://d1f6473b-9f7a-4352-940b-a8d74461d3fc@onelake.dfs.fabric.microsoft.com/d75873bd-716b-4378-a37e-864c76fc9b3b/Tables/raw/orders",
  "workspaceId": "d1f6473b-9f7a-4352-940b-a8d74461d3fc",
  "workspaceName": "sales-analytics",
  "itemId": "d75873bd-716b-4378-a37e-864c76fc9b3b",
  "itemName": "wh_primary",
  "itemType": "Warehouse"
}
```

Everything comes from the table itself, not from the notebook's own workspace, because a notebook can read and write
warehouses in other workspaces. The workspace is always the one in the table's SQL address. For a write without
`WorkspaceId`, that address is built from the notebook's workspace (see above), so set `WorkspaceId` when you write to
another workspace. Outside Fabric, for example when you read a warehouse over JDBC from your own Spark cluster, the
facet has the same fields except `workspaceName`.

To leave the facet out, add it to the disabled facets: `spark.openlineage.facets.fabric_onelake.disabled=true`.

## Lakehouse tables

Spark reads and writes lakehouse tables through their files in OneLake, so it sees them as paths, such as
`abfss://{workspace id}@onelake.dfs.fabric.microsoft.com` / `{lakehouse id}/Tables/sales/orders`. A path doesn't say
whether the item is a lakehouse or a warehouse, so OpenLineage names a table as a lakehouse table only when the path
shows it:

| Namespace                           | Name                           |
|:------------------------------------|:-------------------------------|
| `fabric-lakehouse://{workspace id}` | `{lakehouse}.{schema}.{table}` |

- **The notebook's default lakehouse.** A path whose item ID is the default lakehouse's (`trident.lakehouse.id`) is
  named with `trident.lakehouse.name`. This covers tables written with `saveAsTable`, `CREATE TABLE … AS SELECT`,
  `INSERT INTO` and `MERGE INTO`, and tables read or written by path.
- **Paths written with names.** A path such as `abfss://my-ws@onelake.dfs.fabric.microsoft.com/lh_sales.Lakehouse/Tables/…`
  is named as a lakehouse table when its workspace is the notebook's (`trident.workspace.name`), so that the
  workspace's ID is known.

Lakehouses without schemas use the schema `dbo`. When the lakehouse's ID is known, the table's OneLake location,
written with IDs, is added as a `LOCATION` symlink.

A warehouse table reached by a path written with names, such as
`abfss://my-ws@onelake.dfs.fabric.microsoft.com/wh_sales.Warehouse/Tables/dbo/orders` (or `….Datawarehouse/…`), in the
notebook's workspace, gets the name the connector gives warehouse tables:
`fabric-warehouse://{SQL address}` / `{warehouse}.{schema}.{table}`, with the address [built](#how-the-address-is-built) from
`trident.tenant.id` and the workspace's ID.

Every other OneLake path keeps its path as the name: tables in other lakehouses, warehouse tables reached by ID, files under `Files/`,
and anything outside Fabric, where there are no `trident.*` settings. These still get the `fabric_onelake` facet with
the workspace and item IDs and the location from the path, and the workspace's name when it is the notebook's.


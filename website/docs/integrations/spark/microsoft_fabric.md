---
sidebar_position: 9
title: Microsoft Fabric Warehouse
---

When you read or write a Fabric warehouse table with Microsoft's Spark connector (`synapsesql`), OpenLineage reports
the table itself, named as the [naming conventions](../../spec/naming.md) describe:

| Namespace                                                                           | Name                           |
|:------------------------------------------------------------------------------------|:-------------------------------|
| `fabric-warehouse://{workspace SQL address}`, such as `fabric-warehouse://6obax…-ac4e….datawarehouse.fabric.microsoft.com` | `{warehouse}.{schema}.{table}` |

For example, `spark.read.synapsesql("wh_primary.raw.orders")` is reported as reading
`fabric-warehouse://6obaxtbopqfublwdicr5oicqlm-ac4ezc2eudhehd3wgnlpocnuna.datawarehouse.fabric.microsoft.com` /
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

## Writes

The connector writes a table in two steps:

1. Spark saves the data as Parquet files in a temporary folder.
2. The connector loads those files into the table with `COPY INTO`. This step runs outside Spark.

OpenLineage only sees the first step. It reports the table being loaded as the output, not the temporary folder.

The first step doesn't include the workspace's SQL address, so OpenLineage works it out in this order:

1. If you set the address in `spark.datawarehouse.{warehouse}.sqlendpoint`, that address is used.
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
    .option(Constants.WorkspaceId, "8b4cb800-a044-43ce-8f76-3356f709b468")
    .option(Constants.DatawarehouseId, "0fd9a0c2-ee93-46af-aaea-a45c8649ed9d")
    .synapsesql("wh_primary.raw.orders")
)
```

This read gets the symlink `abfss://8b4cb800-a044-43ce-8f76-3356f709b468@onelake.dfs.fabric.microsoft.com` /
`0fd9a0c2-ee93-46af-aaea-a45c8649ed9d/Tables/raw/orders`.

The location always uses IDs, because OneLake refuses paths that mix IDs and names. Without `DatawarehouseId`, the
connector doesn't make the warehouse's ID available, so no location is reported.

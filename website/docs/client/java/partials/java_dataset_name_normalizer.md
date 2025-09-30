import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Sometimes, an object storage path used by a job to read or write data does not represent a proper dataset name.  
To address this, a **dataset name trimmer** can be applied to trim trailing name segments that are not part of the actual dataset name.

### How It Works
- The **trimmed dataset name** becomes the dataset name.
- The **full, non-trimmed dataset name** is stored in the **subset definition facet** as a `LocationSubsetCondition`.

### Why It Matters
This approach is especially useful for input datasets, where multiple paths may point to the same directory.

- The **subset definition facet** captures all directories read.
- This reduces the size of OpenLineage events by avoiding duplication, since otherwise each directory would be treated as a separate dataset.

### Reducing Datasets in Java Client
Datasets are reduced only if:
1. Their names are trimmed to the same dataset name.
2. They share identical facets.

By the default, OpenLineage Java client comes with the following trimmers:
* `io.openlineage.client.dataset.partition.trimmer.DateTrimmer`
* `io.openlineage.client.dataset.partition.trimmer.KeyValueTrimmer`
* `io.openlineage.client.dataset.partition.trimmer.MultiDirTrimmer`
* `io.openlineage.client.dataset.partition.trimmer.YearMonthTrimmer`

The list of the trimmers can be managed by `disabledTrimmers` and `extraTrimmers` configuration parameters.

In most cases, trimmers work on the last directory segment of the dataset name. 
The trimming process runs iteratively, applying trimmers repeatedly until no additional segments can be removed.

### Trimmers Configuration

<Tabs groupId="async">
<TabItem value="yaml" label="Yaml Config">

```yaml
dataset:
  disabledTrimmers: io.openlineage.client.dataset.partition.trimmer.DateTrimmer
  extraTrimmers: org.company.CustomTrimmer
```
</TabItem>
<TabItem value="spark" label="Spark Config">

| Parameter                                  | Definition                                  | Example                                                       |
---------------------------------------------|---------------------------------------------|---------------------------------------------------------------|
| spark.openlineage.dataset.disabledTrimmers | Semicolon separated list of trimmer classes | `io.openlineage.client.dataset.partition.trimmer.DateTrimmer` |
| spark.openlineage.dataset.extraTrimmers    | Semicolon separated list of trimmer classes | `org.company.CustomTrimmer`                                   |

</TabItem>
<TabItem value="flink" label="Flink Config">

| Parameter                            | Definition                                  | Example                                                       |
---------------------------------------|---------------------------------------------|---------------------------------------------------------------|
| openlineage.dataset.disabledTrimmers | Semicolon separated list of trimmer classes | `io.openlineage.client.dataset.partition.trimmer.DateTrimmer` |
| openlineage.dataset.extraTrimmers    | Semicolon separated list of trimmer classes | `org.company.CustomTrimmer`                                   |

</TabItem>
</Tabs>

### Out of the box trimmers

#### DateTrimmer

Remove a trailing date partition. It checks if the last part of the dataset name contains a valid and recognized date pattern.
Then it checks if the other characters in the directory are only numeric and non-numeric `T` and `Z` characters.
This behaviour assures agility to detect dates beyond the common formats configured in the trimmer.

* `.../20250901/` → trims `/20250901/`
* `.../2025-09-01/` → trims `/2025-09-01/`
* `.../20250722T901Z/` → trims `/20250722T901Z/` as it contains a valid date pattern with extra digits and non-numeric `T` and `Z` characters only.
* `.../2025-25-01/` → trims nothing as it is not a valid date
* `.../dt=2025-09-01/` → may be handled by KeyValueTrimmer

#### KeyValueTrimmer

Remove last part of the dataset name if it follows `key=value` pattern.

* `.../dt=2025-09-01/` → trims `/dt=2025-09-01/`
* `.../hour=05/` → trims `/hour=05/`

#### MultiDirDateTrimmer

Trims multiple directories at once if they are valid date or year month.

* `.../2025/09/01/` → trims `/2025/09/01/`
* `.../2025/09/` → trims `/2025/09/`

#### YearMonthTrimmer

Trims trailing directory if it is a valid year and month.

* `.../202509/` → trims `/2025/09/`
* `.../202533/` → trims nothing
* `.../2025-09/` → trims `/2025-09/`
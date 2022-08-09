# Partitioning proposal

Partitioning is a concept of dividing datasets into parts based on values of particular columns like date, city, etc. For Apache Hive and Spark partitioning affected physical location of data, as each partition is stored in a separate directory. This serves in a similar way to database indexing as accessing certain partitions requires reading only some of data directories. 

It is a common usecase to have a large dataset containing data collected over years, while most of the processing require only recent data to proceed, like the last day. Based on that, partitioning is an important concept within data processing and deserves proper OpenLineage represantion.

## Dataset partitioning columns

Specifying partition columns is part of a dataset schema. That's why it should extend existing `SchemaDatasetFacet` and enrich it with an array of `partitionFields` containing names of some of the schema fields. Providing a `partitionFields` would give an extra information on how is the dataset organised. `partitionFields` should be collected for both input and output datasets. 

## Partitions being modified and accessed

Although the dataset may contain a plenty of partitions, only a small subset of them is read or written most of the time. However, it is difficult to find the proper balance for collecting such information. For example, providing an information on partitions read, may result in thousands of useless entries sent within a single OL event. On the other hand, if a certain partition is affected by an upstream issue, it is valuable to know it. 

It may be then worth to collect at least information on created/dropped partiions:
```
ALTER TABLE table_identifier ADD PARTITION ... 
ALTER TABLE table_identifier DROP PARTITION ...
```

This could be sent within `PartitionsDatasetFacet`:
```
partitions: {
    affected: 
            [
                {
                    col1: value1,
                    col2: value2, 
                    ...
                }
            ],
    operationType: ADD|DROP|WRITE
}
```

## Partition date as a run facet 

Partitioning concept is often related with job frequency and scheduling although the term `partitioning` does not apply well within this context. If an enriched dataset is partitioned daily or hourly, then the job run to append the data is often run daily or hourly. 

OpenLineage should be capable of colleting such information. Otherwise, it is tempting to include partition name in job name which can lead to several duplicate jobs. This can be solved by an adding an extra `RunParameterRunFacet` facet within `RunFacet` to store any:
```
{
    key1: value1,
    key2: value2
}
```
key/value parameters describing the job run. 
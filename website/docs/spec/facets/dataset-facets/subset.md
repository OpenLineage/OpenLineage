---
sidebar_position: 8
---

# Subset Definition Facets

This page demonstrates a list of facets that describe a subset of a dataset being read or written. 
They all extend `BaseSubsetDatasetFacet` and depending if it's an input or output dataset, 
they extend `InputSubsetInputDatasetFacet` or `OutputSubsetOutputDatasetFacet`.

`InputDatasetFacet` has a required `inputCondition` property, while `OutputDatasetFacet` has a required `outputCondition` property.
Both conditions are of type `BaseSubsetCondition` and the implemented conditions are common for inputs and outputs.

Currently, the following subset conditions are available:
* `LocationSubsetCondition` for listing locations like object storage directories,
* `PartitionSubsetCondition` to describe partitioning alike subset definition,
* `CompareSubsetCondition`  to describe logical conditions on dataset fields compared with literal values,
* `BinarySubsetCondition` to describe logical binary operations on the existing conditions.

## LocationSubsetCondition

Useful approach to describe a job that reads certain directories from an object storage.
Using this facet allows limiting the OpenLineage event payload as several similar input datasets
can be reduced into a single dataset with a list of locations.

```json
{
  "subset": {
    "inputCondition": {
      "type": "location",
      "locations": ["s3://some/bucket/location1", "s3://some/bucket/location2", "s3://some/bucket/location3"]
    },
    "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
    "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/BaseSubsetDatasetFacet.json#/$defs/InputSubsetDatasetFacet"
  }
}

```

## PartitionSubsetCondition

Allows defining a subset by a list of partitions. Each partition is defined by its dimensions' values.

```json
{
  "subset": {
    "inputCondition": {
      "type": "partition",
      "partitions": [
        {
          "identifier": "2024-10-15-PL",
          "dimensions": {
            "business_date": "2024-10-15",
            "country": "PL"
          }
        },
        {
          "dimensions": {
            "business_date": "2024-10-15",
            "country": "DE"
          }
        }
      ]
    },
    "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
    "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/BaseSubsetDatasetFacet.json#/$defs/InputSubsetDatasetFacet"
  }
}
```

## `CompareSubsetCondition` and `BinarySubsetCondition`

The combination of `CompareSubsetCondition` and `BinarySubsetCondition` allows describing complex 
logical conditions which are common for SQL `WHERE` clauses.

For example the facet below describes a condition `first_name = 'John' AND last_name = 'Smith'`.

```json
{
  "subset": {
    "inputCondition": {
      "type": "binary",
      "left": {
        "type": "compare",
        "left": {
          "type": "field",
          "field": "first_name"
        },
        "right": {
          "type": "literal",
          "value": "John"
        },
        "comparison": "EQUAL"
      },
      "right": {
        "type": "compare",
        "left": {
          "type": "field",
          "field": "last_name"
        },
        "right": {
          "type": "literal",
          "value": "Smith"
        },
        "comparison": "EQUAL"
      },
      "operator": "AND"
    },
    "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
    "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/BaseSubsetDatasetFacet.json#/$defs/InputSubsetDatasetFacet"
  }
}

```

---
Author: Paweł Leszczyński 
Created: January tenth, 2025 
---

# OpenLineage dataset partitioning proposal

Lineage metadata should contain information needed to answer the following questions:
1. Is the dataset partitioned? How is it done?
2. What partitions have been read by a particular run?
3. What partitions have been updated by a particular run?

There are two problems with answering (2). 
First, it seems to be difficult to extract this information from running jobs. 
Secondly, partition filtering is just a sub problem of describing a subset of the input dataset that has been used.

## Proposed spec changes

### Facet to specify a dataset is partitioned

Create `PartitionDatasetFacet`
```json
{
  "PartitionDatasetFacet": {
    "allOf": [
      {
        "$ref": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/DatasetFacet"
      },
      {
        "type": "object",
        "properties": {
          "dimensions": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "fields": {
                  "type": "array",
                  "items": "string",
                  "description": "Name of the fields used in partitioning."
                },
                "transform": {
                  "type": "string"
                }
              }
            }
          },
         "sqlClause": {
            "type": "string",
            "description": "Optional SQL clause definition like: 'PARTITIONED BY (days(ts), category)'."
         }
        },
        "required": ["dimensions"]
      }
    ]
  }
}
```

Examples:

 * (1) Simple partitioning by two columns: `business_date` and `country`

```json
{
  "partitions": {
    "dimensions": [
      {
        "fields": [
          "busniess_date"
        ],
        "transform": "identity"
      },
      {
        "fields": [
          "country"
        ],
        "transform": "identity"
      }
    ], 
     "sqlClause": "PARTITION BY ('business_date', 'country')" 
  }
}
```
 * (2) partitioning by `date(event_time)` field
```json
{
  "partitions": {
    "dimensions": [
      {
        "fields": [
          "event_time"
        ],
        "transform": "date(event_time)"
      }
    ]
  }
}
```
 * (3) partition by a two-parameter method `foo(col_a, col_b)` and `bar(col_c)`
```json
{
  "partitions": {
    "dimensions": [
      {
        "fields": [
          "col_a",
          "col_b"
        ],
        "transform": "foo(col_a, col_b)"
      },
      {
        "fields": [
          "col_c"
        ],
        "transform": "bar(col_c)"
      }
    ]
  }
}
```

### Input filtering dataset facet

inputFacets should be used to express dataset facets related to a particular run. 
A condition for partition reading is just a special case example of all possible dataset filtering. 
This document is not trying to solve a full problem of describing all the possible dataset filters, 
however a facet introduced needs to be extended easily.

```json
{
   "$schema": "https://json-schema.org/draft/2020-12/schema",
   "$id": "https://openlineage.io/spec/facets/1-0-2/FilterInputDatasetFacet.json",
   "$defs": {
      "BaseFilterCondition": {},
      "BinaryFilterCondition": {
         "allOf": [
            {
               "$ref": "BaseFilterCondition"
            },
            {
               "type": "object",
               "properties": {
                  "left": {
                     "type": "BaseFilterCondition"
                  },
                  "right": {
                     "type": "BaseFilterCondition"
                  },
                  "operator": {
                     "enum": [
                        "OR",
                        "AND"
                     ]
                  }
               }
            }
         ]
      },
      "LiteralComparisonCondition": {
         "allOf": [
            {
               "$ref": "BaseFilterCondition"
            },
            {
               "type": "object",
               "properties": {
                  "field": {
                     "type": "String"
                  },
                  "value": {
                     "type": "String"
                  },
                  "comparison": {
                     "enum": [
                        "EQUAL",
                        "GREATER_THAN",
                        "GREATER_EQUAL_THAN",
                        "LESS_THAN",
                        "LESS_EQUAL_THAN"
                     ]
                  }
               }
            }
         ]
      },
      "FilterInputDatasetFacet": {
         "allOf": [
            {
               "$ref": "https://...OpenLineage.json#/$defs/InputDatasetFacet"
            },
            {
               "type": "object",
               "properties": {
                  "condition": {
                     "type": "#/$defs/BaseFilterCondition"
                  }
               }
            }
         ]
      }
   },
   "type": "object",
   "properties": {
      "filter": {
         "$ref": "#/$defs/FilterInputDatasetFacet"
      }
   }
}

```



An example filter facet for SQL `SELECT * FROM table WHERE col1 = 7 and col2 = 9` would be:

```json
{
  "filter": {
    "$id": "https://openlineage.io/spec/facets/1-0-0/BinaryFilterCondition.json",
    "left": {
      "field": "col1",
      "value": "7",
      "comparison": "EQUAL"
    },
    "right": {
      "field": "col2",
      "value": "9",
      "comparison": "EQUAL"
    },
    "operator": "AND"
  }
}
```
    
Please mind that this approach also allows providing a list of partitions, as well as extending it to add regex patterns on partitioning read.
Output partitions dataset facet

```json
{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://openlineage.io/spec/facets/1-0-0/FilterInputDatasetFacet.json",
    "$defs": {
        "PartitionOutputDatasetFacet": {
            "allOf": [
                {
                    "$ref": "#/$defs/OutputDatasetFacet"
                },
                {
                    "type": "object",
                    "updated": {
                      "type": "array",
                      "items": {
                        "types": "object",
                        "additionalProperties": true
                      }
                    }
                }
           ]
        }
    },
    "type": "object",
    "properties": {
        "partitions": {
          "$ref": "#/$defs/PartitionOutputDatasetFacet"
        }
    }
}
```

An example of this would be a dataset partitioned by `business_date` and `country`. A job run affecting many countries for a single date would affect multiple partitions and this would be described with a facet:

```json
{
  "partitions": {
    "updated": [
      {
        "business_date": "2024-10-15",
        "country": "PL"
      },
      {
        "business_date": "2024-10-15",
        "country": "DE"
      }
    ]
  }
}
```


## Reference implementation with Apache Spark

 * Support extracting dataset partitioning info from:
   * `DataSourceV2Relation objects`, see partitioning field ([see code](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/Table.java#L71)),
   * [Iceberg partitioning](https://iceberg.apache.org/docs/1.6.1/partitioning/#problems-with-hive-partitioning).
 * Verify how dataset filtering is reflected in `LogicalPlan`. Produce filter input facets.
 * Support configuration based partitioning:
   * Provide configuration to manually specify how datasets are partitioned within the organization.
   * For example, if a configuration contains `business_date` partitioning definition, then a Spark job reading data from `s3://bucket/dataset/business_date=20241101` would replace dataset identifier with `s3://bucket/dataset` and `FilterInputDatasetFacet` determining `business_date` specified.
   * Similarly, a job writing a location `s3://bucket/dataset/business_date=20241101` would identify a dataset `s3://bucket/dataset` and enrich it with `PartitionOutputDatasetFacet`.


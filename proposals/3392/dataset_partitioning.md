---
Authors: Paweł Leszczyński, Maxim Martynov
Created: January tenth, 2025 
---

# OpenLineage dataset partitioning proposal

Lineage metadata should contain information needed to answer the following questions:
1. Is the dataset partitioned? How is it done?
2. What partitions have been read by a particular run?
3. What partitions have been updated by a particular run?

Partition filtering is just a sub problem of describing a subset of the input dataset that has been used.
We propose `SubsetDatasetFacet` to answer (2) and (3) while keeping in mind, that it may be used in other contexts as well.

## PartitionDatasetFacet

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
         "partitionedBySqlClause": {
            "type": "string",
            "description": "Optional SQL clause definition like: '(days(ts), category)' for SQL clause `PARTITIONED BY (days(ts), category)` used."
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
          "business_date"
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
     "sqlClause": "('business_date', 'country')" 
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

## SubsetDatasetFacet

It can be argued if a `SubsetDatasetFacet` should be a part of partitioning proposal. 
The counterargument to this, is that it makes more sense to discuss partitioning 
with a facet that is capable of describing partitions being read or written to.
Otherwise, it is easy to miss the practical context of discussing partitioning.

`SubsetDatasetFacet` may describe either `input` or `output` dataset. 
Possible scenarios to cover include: 
 * SQL query filtering: `WHERE a = '...' AND b = '...'`,
 * Job reading a path defined with regex `/some/path/a=*/b=1`,
 * Job writing to `s3://bucket/dataset/business_date=20241101`,
 * Job reading to a list of partitions,
 * Job writing to a list of partitions,
 * Job reading a dataset by a predicate on partitioned fields.

### Base subset dataset facet

From the examples above, we can distinguish logical and physical definitions
of a subset. A proposed facet should be capable of storing both definitions at
the same time as sometimes reading a path `/some/b=1` may be equivalent
to logical filtering `b=1`. The physical notion describes the actual location while
the logical describes a condition applied. 

```json
{
   "BaseSubsetDatasetFacet": {
      "allOf": [
         {
            "$ref": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/DatasetFacet"
         },
         {
            "type": "object",
            "properties": {
               "logical": {
                  "$ref": "#/$defs/LogicalSubsetDefinition"
               },
               "physical": {
                  "$ref": "#/$defs/PhysicalSubsetDefinition"
               }
            }
         }
      ],
      "type": "object",
      "properties": {
         "subset": {
            "$ref": "#/$defs/BaseSubsetDatasetFacet"
         }
      }
   }
}
```

A subset dataset facet can be used either as `InputDatasetFacet` and `OutputDatasetFacet`,
however it cannot extend both objects at the same time.
To solve this, we introduce:
```json
{
   "InputSubsetDatasetFacet": {
      "allOf": [
         {
            "$ref": "#/$defs/InputDatasetFacet"
         },
         {
            "$ref": "#/$defs/BaseSubsetDatasetFacet"
         }
      ]
   },
   "OutputSubsetDatasetFacet": {
      "allOf": [
         {
            "$ref": "#/$defs/OutputDatasetFacet"
         }
      ]
   }
}
```

## Logical subset definition

```json
{
   "LogicalSubsetDefinition": { },
   "BinarySubsetCondition": {
      "allOf": [
         {
            "$ref": "LogicalSubsetDefinition"
         },
         {
            "type": "object",
            "properties": {
               "left": {
                  "type": "LogicalSubsetDefinition"
               },
               "right": {
                  "type": "LogicalSubsetDefinition"
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
   "LiteralSubsetCondition": {
      "allOf": [
         {
            "$ref": "LogicalSubsetDefinition"
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
   }
}
```

An example subset dataset facet for SQL 
```
SELECT * FROM table WHERE col1 = 7 and col2 = 9
``` 
would be:
```json
{
   "namespace": "dataset-namespace",
   "name": "dataset-name",
   "inputFacets": {
      "subset": {
         "$id": "https://openlineage.io/spec/facets/1-0-0/SubsetDatasetFacet.json",
         "logical": {
            "$id": "https://openlineage.io/spec/facets/1-0-0/BinarySubsetCondition.json",
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
   } 
}
``` 

A logical subset definition can also define a list of partitions read:
```json
{
  "PartitionListSubsetDefinition": {
    "allOf": [
      {
        "$ref": "#/$defs/LogicalSubsetDefinition"
      },
      {
        "type": "object",
        "properties": {
          "partitions": {
            "type": "array",
            "items": {
              "types": "object",
              "properties": {
                "identifier": {
                  "type": "string",
                  "description": "Optionally provided identifier of the partition specified"
                },
                "dimensions": {
                  "type": "object",
                  "additionalProperties": true
                }
              }
            }
          }
        }
      }
    ]
  }
}
```
An example of this would be a dataset partitioned by `business_date` and `country`. 
A job run affecting many countries for a single date would affect multiple 
partitions and this would be described with a facet:

```json
{
   "namespace": "dataset-namespace",
   "name": "dataset-name",
   "outputFacets": {
      "subset": {
         "$id": "https://openlineage.io/spec/facets/1-0-0/SubsetDatasetFacet.json",
         "logical": {
            "$id": "https://openlineage.io/spec/facets/1-0-0/PartitionListSubsetDefinition.json",
            "partitions": [
               {
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
         }
      }
   }
}
```

This concept can be extended in future to describe more complex conditions on
dataset partitions.

## Physical subset definition

```json
{
   "PhysicalSubsetDefinition": { },
   "PathSubsetCondition": {
      "allOf": [
         {
            "$ref": "PhysicalSubsetDefinition"
         },
         {
            "type": "object",
            "properties": {
               "path": {
                  "type": "String"
               }
            }
         }
      ]
   },
   "PathPatternSubsetCondition": {
      "allOf": [
         {
            "$ref": "PhysicalSubsetDefinition"
         },
         {
            "type": "object",
            "properties": {
               "pathPattern": {
                  "type": "String",
                  "example": "/some/path/a=*/b=1"
               }
            }
         }
      ]
   },
   "PathListSubsetCondition": {
      "allOf": [
         {
            "$ref": "PhysicalSubsetDefinition"
         },
         {
            "type": "array",
            "items": {
               "ref": "$defs/PathSubsetCondition"
            }
         }
      ]
   }
}
```

The above definitions can be used to describe reading one specified location,
multiple locations specified with regex or describing a list of specified locations.

## Reference implementation with Apache Spark

 * Support extracting dataset partitioning info from:
   * `DataSourceV2Relation objects`, see partitioning field ([see code](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/Table.java#L71)),
   * [Iceberg partitioning](https://iceberg.apache.org/docs/1.6.1/partitioning/#problems-with-hive-partitioning).
 * Verify how dataset filtering is reflected in `LogicalPlan`. Produce filter input facets based on filtering proposal prepared.
 * Support configuration based partitioning:
   * Provide configuration to manually specify how datasets are partitioned within the organization.
   * For example, if a configuration contains `business_date` partitioning definition, then a Spark job reading data from `s3://bucket/dataset/business_date=20241101` would replace dataset identifier with `s3://bucket/dataset` and `FilterInputDatasetFacet` determining `business_date` specified.
   * Similarly, a job writing a location `s3://bucket/dataset/business_date=20241101` would identify a dataset `s3://bucket/dataset` and enrich it with `PartitionOutputDatasetFacet`.

In the past we often used Spark as a reference implementation. The specific thing about it is
that it gives OpenLineage access to logical plan at the moment of the job running.
Oftentimes, this allows extracting more lineage metadata than in other engines. 
For example, given non-Spark database and a query 
```
INSERT INTO partitioned_table SELECT FROM other table WHERE some condition
```
it may be impossible to extract information about the partitions affected by SQL query. 
However, a table schema should allow exposing table partitioning information. 
SQL text can be also used to extract filtering conditions. 


---
Authors: Paweł Leszczyński, Maxim Martynov, Maciej Obuchowski
Created: February 18, 2025 
---

# OpenLineage dataset partitioning proposal

Lineage metadata should contain information needed to answer the following questions:
1. Is the dataset partitioned? How is it done?
2. What partitions have been read by a particular run?
3. What partitions have been updated by a particular run?

Partition filtering is just a sub-problem of describing a subset of the input dataset that has been used.
We propose subset dataset facet to answer (2) and (3) while keeping in mind, that it may be used in other contexts, like for example
query filtering

## PartitionDatasetFacet

We introduce: `PartitionDatasetFacet`:

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
         "description": {
            "type": "string",
            "description": "Partitioning description, can be SQL definition clause like `PARTITIONED BY (days(ts), category)`."
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
     "description": "PARTITIONED BY ('business_date', 'country')" 
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
    ],
    "description": "PARTITIONED BY (date(event_time))"
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
    ], 
    "description": "PARTITIONED BY foo(col_a, col_b), bar(col_c)"
  }
}
```

## Describing subset of a dataset

It can be argued if a facet to describe dataset's subset should be a part of partitioning proposal. 
However, it makes more sense to discuss partitioning facets
together with a facet that is capable of describing partitions being read or written to.
Otherwise, it is easy to miss the practical context of partitioning metadata.

Subset facet may describe either `input` or `output` dataset. 
Common scenarios to cover include: 
 * SQL query filtering: `WHERE a = '...' AND b = '...'`,
 * Job reading/writing to specified path `s3://bucket/dataset/business_date=20241101`,
 * Job reading a list of partitions,
 * Job writing to a specified partition,
 * Job reading a dataset by a predicate on partitioned fields.

We start with logical subset definitions:

```json
{
   "BaseSubsetCondition": {
      "type": "object",
      "properties": {
         "type": "string"
      }
   },
   "BinarySubsetCondition": {
      "allOf": [
         {
            "$ref": "BaseSubsetCondition"
         },
         {
            "type": "object",
            "properties": {
               "left": {
                  "type": "BaseSubsetCondition"
               },
               "right": {
                  "type": "BaseSubsetCondition"
               },
               "type": {
                  "const": "binary"
               },
               "operator": {
                  "type": "string",
                  "example": "AND",
                  "description": "Allowed values: 'AND' or 'OR'"
               }
            }
         }
      ]
   },
   "CompareSubsetCondition": {
      "allOf": [
         {
            "$ref": "BaseSubsetCondition"
         },
         {
            "type": "object",
            "properties": {
               "type": {
                  "const": "compare"
               },
               "left": {
                  "type": "CompareExpression"
               },
               "right": {
                  "type": "CompareExpression"
               },
               "comparison": {
                  "type": "string",
                  "example": "EQUAL",
                  "description": "Allowed values: 'EQUAL', 'GREATER_THAN', 'GREATER_EQUAL_THAN', 'LESS_THAN', 'LESS_EQUAL_THAN'",
               }
            }
         }
      ]
   },
   "CompareExpression": {
      "type": "object"
   },
   "LiteralCompareExpression": {
      "allOf": [
         {
            "$ref": "CompareExpression"
         },
         {
            "type": "object",
            "properties": {
               "type":  {
                  "const": "literal"
               },
               "value": "string"
            }
         }
      ]
   },
   "FieldCompareExpression": {
      "allOf": [
         {
            "$ref": "CompareExpression"
         },
        {
           "type": "object",
           "properties": {
              "type":  {
                 "const": "field"
              },
              "field": "string"
           }
        }
      ]
   },
   "TransformFieldCompareExpression": { }
}
```

This allows us to describe logical conditions like:
 * `a = 1 AND b = 2`,
 * `a = 1 OR b = 2`,
 * `a > 1 AND b < 2`.

It also allows to describe complex conditions like: `a = 1 AND (b = 2 OR c = 3)`
as `BinarySubsetCondition` can be nested. `TransformFieldCompareExpression` can be introduced
later to describe condition on the transformed fields, like `func(c)=6`

This model can be extended easily. For the context of this proposal, we want to be able
to specify a list of partitions:
```json
{
   "PartitionSubsetCondition": {
      "allOf": [
         {
            "$ref": "#/$defs/BaseSubsetCondition"
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
                        "type":  {
                           "const": "partition"
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

Additional to logical subset definition, there is a need to describe physical locations. 
This can be:
```json
{
   "LocationSubsetCondition": {
      "allOf": [
         {
            "$ref": "#/$defs/BaseSubsetCondition"
         },
         {
            "type": "object",
            "properties": {
               "type":  {
                  "const": "location"
               },
               "locations": {
                    "type": "array",
                    "items": {
                      "type": "string"
                    }
               }
            }
         }
      ]
   }
}
```

A Spark job may read dataset from a location with a wildcard, like `/some/path/a=*/b=1`. In this case
we assume, subset definition can be resolved from physical plan when wildcard gets resolved into a 
list of files.

In some cases, like  SQL queries, a subset definition can be too complex to be represented in a structured
way. However, this still makes sense to include a description of it.

```json
{
   "DescriptionSubsetCondition": {
      "allOf": [
         {
            "$ref": "#/$defs/BaseSubsetCondition"
         },
         {
            "type": "object",
            "properties": {
               "type":  {
                 "const": "description"
               },
               "description": "String"
            }
         }
      ]
   }
}
```

In other words, we accept OpenLineage will not fully understand SQL syntax, like the syntax below:
```sql
SELECT *
FROM Employees
WHERE EXISTS (SELECT * FROM EmployeeReviews er  WHERE er.EmployeeID = Employees.EmployeeID AND er.Rating > 4);
```
However, even in this case, OpenLineage should be able to provide informative subset definition:
```json
{
   "subset": {
      "type": "description",
      "description": "EXISTS (SELECT * FROM EmployeeReviews er  WHERE er.EmployeeID = Employees.EmployeeID AND er.Rating > 4)"
   }
}
```

### Subset dataset facet

Supplied with a subset condition objects, we can define subset dataset facets.
Facets for inputs need to `InputDatasetFacet`, while the output has to extend `OutputDatasetFacet`, 
which requires us to provide to separate facet definitions:

```json
{
   "InputSubsetDatasetFacet": {
      "allOf": [
         {
            "$ref": "#/$defs/InputDatasetFacet"
         },
         {
            "type": "object",
            "properties": {
               "condition": {
                  "$ref": "#/$defs/BaseSubsetCondition"
               }
            }
         }
      ]
   },
   "OutputSubsetDatasetFacet": {
      "allOf": [
         {
            "$ref": "#/$defs/OutputDatasetFacet"
         },
         {
            "type": "object",
            "properties": {
               "condition": {
                  "$ref": "#/$defs/BaseSubsetCondition"
               }
            }
         }
      ]
   }
}
```

In many cases, the same subset can be defined through different subset conditions.
When implementing a strategy to describe a dataset subset definition, following hierarchy should be used:

 * If a subset can be identified by partitions, use this as subset definition first. A single partition should be a minimal granularity. If a single partition is additionally filtered by some additional logical condition, the logical condition will not be included. Producer may implement a configurable maximal amount of partitions collected. If this gets exceeded, this hierarchy step is skipped.
 * If a subset can be identified by physical locations, like directories or files (with no wildcards), use this as subset definitions. Additional logical conditions are skipped. Producer may implement a configurable maximal amount of locations collected. If this gets exceeded, this hierarchy step is skipped.
 * We describe a subset definition by a combination of logical conditions if the approach is applicable.
 * In case of unrecognized subset definition, a `DescriptionSubsetCondition` is used.

For two first hierarchy levels, there is a possible scenario of a job reading thousands of partitions or files. 
We allow producer to have a configurable maximal amount of partitions or locations collected, as at some point this information stops being useful. 

Table bucketing may also be a valid partitioning strategy. It's not listed within this proposal,
as it is not certain if any OpenLineage producer can extract buckets read or updated within the job.
If this is doable, then a hierarchy should be extended to support `bucket` subset definition.

### Examples

An example subset dataset facet for SQL:
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
       "type": "binary",
       "left": {
          "type": "compare", 
          "left": {
            "type": "field", "value": "col1"
          },
          "right": {
             "type": "literal", "value": "7"
          },
          "comparison": "EQUAL"
       },
       "right": {
          "type": "compare",
          "left": {
             "type": "field", "value": "col2"
          },
          "right": {
             "type": "literal", "value": "9"
          },
          "comparison": "EQUAL"
       },
       "operator": "AND"
     }
   }
}
``` 

An example of this would be a dataset partitioned by `business_date` and `country`. 
A job affecting many countries for a single date would affect multiple 
partitions and this would be described with a facet:

```json
{
   "namespace": "dataset-namespace",
   "name": "dataset-name",
   "outputFacets": {
      "subset": {
         "type": "partition",
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
```

A spark job:
```python
spark \
  .read \
  .parquet("/some/path/a=*/b=1") \
  .filter(col("c") == 2)
```

would result in:

```json
{
   "namespace": "dataset-namespace",
   "name": "dataset-name",
   "inputFacets": {
      "subset": {
         "type": "location",
         "locations": [
            "/some/path/a=1/b=1",
            "/some/path/a=2/b=1",
            "/some/path/a=3/b=1",
            "/some/path/a=4/b=1"
         ]
      }
   }
}
```

If the number of locations exceeds a configurable threshold, the facet would be:
```json
{
   "namespace": "dataset-namespace",
   "name": "dataset-name",
   "inputFacets": {
      "subset": {
         "type": "compare",
         "left": {
            "type": "field", "value": "c"
         },
         "right": {
            "type": "literal", "value": "2"
         },
         "comparison": "EQUAL"
      }
   }
}
```

## Reference implementation with Apache Spark

 * Support extracting dataset partitioning info from:
   * `DataSourceV2Relation objects`, see partitioning field ([see code](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/Table.java#L71)),
   * [Iceberg partitioning](https://iceberg.apache.org/docs/1.6.1/partitioning/#problems-with-hive-partitioning).
 * Verify how dataset filtering is reflected in `LogicalPlan`. Produce subset condition input facets based on filtering proposal prepared.
 * Support configuration based partitioning:
   * Provide configuration to manually specify how datasets are partitioned within the organization.
   * For example, if a configuration contains `business_date` partitioning definition, then a Spark job reading data from `s3://bucket/dataset/business_date=20241101` would replace dataset identifier with `s3://bucket/dataset` and `FilterInputDatasetFacet` determining `business_date` specified.
   * Similarly, a job writing a location `s3://bucket/dataset/business_date=20241101` would identify a dataset `s3://bucket/dataset` and enrich it with `InputSubsetDatasetFacet`.

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


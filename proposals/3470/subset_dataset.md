---
Authors: Paweł Leszczyński
Created: February sixth, 2025 
---

# Subset dataset facet

Oftentimes, an integration can deduct a subset of dataset that is being read or written. 
For example, if a job reads only recent data, it can be useful to include this information within
OpenLineage events. If a dataset is partitioned daily, and a job write a single partition, it is 
useful to include this information as lineage metadata. 

Subset dataset facet should serve the following use cases: 
 * Describe filtering applied to the input dataset.
 * Describe input partitions read by a dataset if an integration can extract such information.
 * Describe output partitions written by a dataset if an integration can extract such information.
 * Provide a meaningful way to describe the subset of data being updated.

```json
{
   "$schema": "https://json-schema.org/draft/2020-12/schema",
   "$id": "https://openlineage.io/spec/facets/1-0-2/SubsetDatasetFacet.json",
   "$defs": {
      "BaseSubsetCondition": {},
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
               "$ref": "BaseSubsetCondition"
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
      "SubsetDatasetFacet": {
         "allOf": [
            {
               "$ref": "https://...OpenLineage.json#/$defs/DatasetFacet"
            },
            {
               "type": "object",
               "properties": {
                  "condition": {
                     "type": "#/$defs/BaseSubsetCondition"
                  }
               }
            }
         ]
      }
   },
   "type": "object",
   "properties": {
      "subset": {
         "$ref": "#/$defs/SubsetDatasetFacet"
      }
   }
}

```


An example filter facet for SQL `SELECT * FROM table WHERE col1 = 7 and col2 = 9` would be:

```json
{
  "filter": {
    "$id": "https://openlineage.io/spec/facets/1-0-0/SubsetDatasetFacet.json",
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
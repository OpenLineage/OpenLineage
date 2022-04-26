<!-- SPDX-License-Identifier: Apache-2.0 -->

---
Status: IN PROGRESS  
Author: Julien Le Dem (original issue) / Pawel Leszczynski (this document)  
Created: 2022-04-01  
Issue: [148](https://github.com/OpenLineage/OpenLineage/issues/148)
---

## Purpose

Column level lineage provides fine grained information on datasets' dependencies. Not only we know the dependency exist, but we are also able to understand which input columns are used to produce output columns. This allows answering questions like ‘which root input columns are used to construct column x?’. This document describes an idea how to capture this information in the OpenLineage model.

## Proposed implementation

We propose to introduce a new dataset facet `columnLineage`. For each output, this will contain list of output's fields with input fields used to evaluate it. The input fields are identified by: namespace, name and field.

```json
{
   "eventType": "COMPLETE",
   "eventTime": "2020-12-28T20:52:00.001+10:00",
   "run" : {
     "runId": "uuid"
   },
  "job": {
     "namespace": "scheduler",
     "name": "myjob",
     "facets": {
        "sql": {
            "query": "Insert into outputTable from select * from inputTable"
        }
     }
   },
   "inputs": [
     { 
       "namespace": "N1",
       "name": "inputTable",
       "facets": {
        "schema": {
           "fields": [ {"name": "col_a", "type": "VARCHAR"},  {"name": "col_b", "type": "int"}]
         }
       }
    }
   ],
   "outputs": [
     { 
       "namespace": "N2",
       "name": "outputTable",
       "facets": {
        "schema": {
           "fields": [ {"name": "col_a", "type": "VARCHAR"}, {"name": "col_b", "type": "int"} ]
         },
         "columnLineage": {
           "fields": {
             "col_a": {
               "inputFields": [ { "namespace": "N1", "name": "inputTable", "field": "col_a"} ]
             },
             "col_b": {
               "inputFields": [ { "namespace": "N1", "name": "inputTable", "field": "col_b"} ]
             }
           }
         }
       }
     }
   ]
}
```

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://openlineage.io/spec/facets/1-0-0/ColumnLineageDatasetFacet.json",
  "$defs": {
    "ColumnLineageDatasetFacet": {
      "allOf": [{
        "$ref": "https://openlineage.io/spec/1-0-2/OpenLineage.json#/$defs/DatasetFacet"
      }, {
        "type": "object",
        "properties": {
          "fields": {
            "description": "Column level lineage that maps output fields into input fields used to evaluate them.",
            "type": "object",
            "additionalProperties": {
              "type": "object",
              "properties": {
                "inputFields": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "namespace": {
                        "type": "string",
                        "description": "The input dataset namespace"
                      },
                      "name": {
                        "type": "string",
                        "description": "The input dataset name"
                      },
                      "field": {
                        "type": "string",
                        "description": "The input field"
                      }
                    },
                    "required": [
                      "namespace", "name", "field"
                    ]
                  }
                }
              },
              "required": ["inputFields"]
            }      
          }    
        },
        "additionalProperties": true,
        "required": [
          "fields"
        ]
      }],
      "type": "object"
    }
  },
  "type": "object",
  "properties": {
    "columnLineage": {
      "$ref": "#/$defs/ColumnLineageDatasetFacet"
    }
  }
}
```


## Spark Implementation

We will use OpenLineage Spark integration to test `columnLineage` in action. 

OpenLineage & Spark integration relies on implementing visitors that traverse Spark LogicalPlan and extract meaningful information when encountered. We do already identify the outputs and the inputs with their schemas. Attributes within LogicalPlan's nodes contain `ExprId` which define the dependencies between attributes of adjacent nodes. Traversing whole `LogicalPlan` allows capturing all the dependencies required to build column level lineage.


# Column level lineage

The [column level lineage facet](ColumnLineageDatasetFacet.json) captures the lineage of columns of an output dataset
from the columns in input datasets. It must refer to existing columns as defined in the
[`schema` facet](SchemaDatasetFacet.json). Additional information on the transformation from the input columns to the
output column is stored in the optional _transformationDescription_ and _transformationType_ fields.

- _transformationDescription_: a human readable description of the transformation. ex: "(a+b)" or "identical"
- _transformationType_: type of the transformation. possible values:
- "IDENTITY" exactly the same as the input.
- "MASKED" for example a hash of the input value that doesn't expose the original value.

Output Dataset example of adding a columnLineage facet:

```diff
    {
      "namespace": "{namespace of the outputdataset}",
      "name": "{name of the output dataset}",
      "facets": {
        "schema": {
          "fields": [
            { "name": "{first column of the output dataset}", "type": "{its type}"},
            { "name": "{second column of the output dataset}", "type": "{its type}"},
            ...
          ]
        },
>       "columnLineage": {
>         "fields": {
>           "{first column of the output dataset}": {
>             "inputFields": [
>               { "namespace": "{input dataset namespace}", name: "{input dataset name}", "field": "{input dataset column name}"},
>               ... other inputs
>             ],
>             "transformationDescription": "identical",
>             "transformationType": "IDENTITY"
>           },
>           "{second column of the output dataset}": ...,
>           ...
>         }
>       }
      }
    }
```

Full lineage event example:

```
{
  "eventType": "START",
  "eventTime": "2020-12-09T23:37:31.081Z",
  "run": {
    "runId": "3b452093-782c-4ef2-9c0c-aafe2aa6f34d",
  },
  "job": {
    "namespace": "my-scheduler-namespace",
    "name": "myjob.mytask",
  },
  "inputs": [
    {
      "namespace": "my-datasource-namespace",
      "name": "instance.schema.table",
      "facets": {
        "schema": {
          "fields": [
            { "name": "ia", "type": "INT"},
            { "name": "ib", "type": "INT"}
          ]
        },
      }
    }
  ],
  "outputs": [
    {
      "namespace": "my-datasource-namespace",
      "name": "instance.schema.output_table",
      "facets": {
        "schema": {
          "fields": [
            { "name": "a", "type": "INT"},
            { "name": "b", "type": "INT"}
          ]
        },
        "columnLineage": {
          "fields": {
            "a": {
              "inputFields": [
                {namespace: "my-datasource-namespace", name: "instance.schema.table", "field": "ia"},
                ... other inputs
              ],
              transformationDescription: "identical",
              transformationType: "IDENTITY"
            },
            "b": ... other output fields
          }
        }
      }
    }
  ],
  "schemaURL": "https://openlineage.io/spec/1-0-0/OpenLineage.json#/definitions/RunEvent"
}
```

---

SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2023 contributors to the OpenLineage project

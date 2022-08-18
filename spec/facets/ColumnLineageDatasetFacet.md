# Column level lineage

The [column level lineage facet](ColumnLineageDatasetFacet.json) captures the lineage of columns of an output dataset from the columns in input datasets.


Example:
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
  ],
  "schemaURL": "https://openlineage.io/spec/1-0-0/OpenLineage.json#/definitions/RunEvent"
}
```

# External Query Run

The [external query run facet](ExternalQueryRunFacet.json) captures the identifier of the query that ran on an external source systems such as [bigquery](https://cloud.google.com/bigquery).

*externalQueryId* and *source* fields.
 - *externalQueryId*: unique identifier of the query from the source system.
 - *source*: type of the source (e.g. bigquery)

Full lineage event example:
```
{
  "eventType": "START",
  "eventTime": "2020-12-09T23:37:31.081Z",
  "run": {
    "runId": "3b452093-782c-4ef2-9c0c-aafe2aa6f34d",
    "facets": {
>        "externalQuery": {
>            "externalQueryId": "my-project-1234:US.bquijob_123x456_123y123z123c",
>            "source": "bigquery"
>        }
    }
  },
  "job": {
    "namespace": "my-namespace",
    "name": "myjob.mytask",
  },
  "inputs": [
    {
      "namespace": "my-datasource-namespace",
      "name": "instance.schema.table",
    }
  ],
  "outputs": [
    {
      "namespace": "my-datasource-namespace",
      "name": "instance.schema.output_table",
    }
  ],
  "schemaURL": "https://openlineage.io/spec/1-0-0/OpenLineage.json#/definitions/RunEvent"
}
```

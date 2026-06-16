# Prefect OpenLineage

The Prefect OpenLineage integration listens for task and flow events using Prefect Event Clients. Information from events is supplemented with metadata from the Prefect API.

## Configuration

`OPENLINEAGE_NAMESPACE` and `PREFECT_API_URL` env variables are both required. Optionally, use a deployment variable to set the namespace variable.

## Datasets

The integration uses Prefect Table Artifacts to add datasets to OpenLineage run events.

Pass a dataset object to an Artifact using `create_table_artifact` from the Artifacts library. Import it in your flow like so: 

```py
from prefect.artifacts import create_table_artifact
```

Datasets are dictionaries requiring a `database_uri` and `table`:

```py
ol_dataset = [{"database_uri":"duckdb:///customers_db", "table":"customers"}]
```

The Artifact's description provides the dataset name and type to the integration. For input datasets, end the description with `_input`, and for output datasets use `_output`: 

```py
create_table_artifact(
    key="ingest-query",
    table=ol_dataset,
    description="ol-dataset_input"
)
```

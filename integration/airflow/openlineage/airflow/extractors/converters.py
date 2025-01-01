# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import Optional, Tuple, cast
from urllib.parse import urlparse

from openlineage.client.event_v2 import Dataset
from openlineage.client.facet_v2 import schema_dataset

from airflow.lineage.entities import File, Table


def convert_from_object_storage_uri(uri: str) -> Optional[Dataset]:
    try:
        scheme, netloc, path, params, _, _ = cast(Tuple[str, str, str, str, str, str], urlparse(uri))
    except Exception:
        return None
    if scheme.startswith("s3"):
        return Dataset(namespace=f"s3://{netloc}", name=path)
    elif scheme.startswith(("gcs", "gs")):
        return Dataset(namespace=f"gs://{netloc}", name=path)
    elif "/" not in uri:
        return None
    return Dataset(namespace=scheme, name=f"/{netloc}{path}")


def convert_to_dataset(obj):
    if isinstance(obj, Dataset):
        return obj
    elif isinstance(obj, Table):
        return Dataset(
            namespace=obj.cluster,
            name=f"{obj.database}.{obj.name}",
            facets={
                "schema": schema_dataset.SchemaDatasetFacet(
                    fields=[
                        schema_dataset.SchemaDatasetFacetFields(
                            name=column.name,
                            type=column.data_type,
                            description=column.description,
                        )
                        for column in obj.columns
                    ]
                )
            }
            if obj.columns
            else {},
        )

    elif isinstance(obj, File):
        return convert_from_object_storage_uri(obj.url)
    else:
        return None

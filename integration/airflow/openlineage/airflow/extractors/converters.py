# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from openlineage.client.run import Dataset

from airflow.lineage.entities import Table


def convert_to_dataset(obj):
    if isinstance(obj, Dataset):
        return obj
    elif isinstance(obj, Table):
        return Dataset(
            namespace=f"{obj.cluster}",
            name=f"{obj.database}.{obj.name}",
            facets={},
        )
    else:
        return None

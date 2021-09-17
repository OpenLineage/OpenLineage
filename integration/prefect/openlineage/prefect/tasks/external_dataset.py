from typing import Optional, Dict

import fsspec
import prefect
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

from openlineage.prefect.adapter import OpenLineageAdapter


class ExternalDataset(Task):
    """
    Task for reading data via `fsspec` and creating an OpenLineage Dataset at the same time.

    Useful if you want a dataset to be available in OpenLineage for lineage purposes, but don't have control over the
    process writing the data, and therefore don't have lineage already set up. An example would be some files on a HTTP
    server or S3 bucket.

    Args:
        - url_path (str, optional): Absolute url to a file. See `fsspec.open` for details on reading for various
            filesystems
        - **kwargs (dict, optional): Additional keyword arguments to pass to the Task constructor

    Example - A file that exists in public S3 bucket:
      t = ExternalDataset(
          source_name="my-data",
          source_type="S3",
          source_connection_url="s3://my-bucket/path/to/file",
        )
    """

    def __init__(
        self,
        source_name: str = None,
        source_type: str = None,
        source_connection_url: str = None,
        source_description: Optional[str] = None,
        dataset_name: str = None,
        dataset_url_path: str = None,
        dataset_kw: Optional[Dict] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.source_name = source_name
        self.source_type = source_type
        self.source_connection_url = source_connection_url
        self.source_description = source_description
        self.dataset_name = dataset_name
        self.dataset_url_path = dataset_url_path
        self.dataset_kw = dataset_kw

    @defaults_from_attrs(
        "source_name",
        "source_type",
        "source_connection_url",
        "source_description",
        "dataset_name",
        "dataset_url_path",
        "dataset_kw",
    )
    def run(
        self,
        source_name: str = None,
        source_type: str = None,
        source_connection_url: str = None,
        source_description: Optional[str] = None,
        dataset_name: str = None,
        dataset_url_path: str = None,
        dataset_kw: Optional[Dict] = None,
    ):
        # Read the data
        with fsspec.open(self.dataset_url_path) as f:
            data = f.read()

        # Create the OpenLineage Source & Dataset
        lineage: OpenLineageAdapter = prefect.context.lineage
        lineage.create_source(
            source_name=source_name,
            source_type=source_type,
            connection_url=f"{source_type.lower()}:{source_connection_url}",
            description=source_description,
        )
        lineage.create_dataset(
            source_name=source_name,
            dataset_name=dataset_name,
            url_path=dataset_url_path,
            **(dataset_kw or {})
        )
        return data

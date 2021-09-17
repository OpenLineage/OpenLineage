from typing import Any

from marquez_client.models import DatasetType
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs


# TODO - Should we return data from this task? Probably


class DatasetTask(Task):
    """
    Task for registering a dataset with Marquez.

    Useful if you want a dataset to be available in Marquez for lineage purposes, but don't have control over the
    process writing the data, and therefore don't have lineage already set up. An example would be some files on a HTTP
    server or S3 bucket.

    Args:
        - image_name (str, optional): Name of the image to run

    """

    def __init__(
        self,
        dataset_type: DatasetType = None,
        **kwargs: Any,
    ):
        self.dataset_type = dataset_type

        super().__init__(**kwargs)

    @defaults_from_attrs(
        "dataset_type",
    )
    def run(
        self,
        dataset_type: DatasetType = None,
    ) -> str:
        pass

import inspect
from functools import lru_cache
from typing import Any, Dict, Tuple, Union

import fsspec
from dask.base import tokenize
from fsspec import AbstractFileSystem
from fsspec.utils import infer_storage_options
from prefect import Parameter
from prefect import Task
from prefect.engine.result import Result
from prefect.engine.serializers import Serializer


@lru_cache()
def get_fs(url) -> Tuple[AbstractFileSystem, str]:
    options = infer_storage_options(urlpath=url)
    fs = fsspec.filesystem(options["protocol"])
    root = url.replace(options["protocol"] + "://", "")
    return fs, root


def task_qualified_name(task: Task):
    return f"{task.__module__}.{task.name}"


def _hash_result(result: Any, serializer: Serializer) -> Dict:
    serialized = serializer.serialize(result)
    token = tokenize(result)
    return {"hash": token, "size": len(serialized)}


def _hash_inputs(inputs: Dict[str, Union[Result, Parameter]]):
    return {
        key: _hash_result(value.value, serializer=value.serializer) for key, value in inputs.items()
    }


def _hash_source(func, name=None):
    """Hash source code for run function, stripping away @task decorator"""
    source = inspect.getsource(func)
    if source.startswith("@task"):
        source = source.split("\n", maxsplit=1)[1]
    assert source.startswith(
        "def"
    ), f"Source for task {name} does not start with def, using decorator?"
    return _hash_result(result=source, serializer=SourceSerializer())


class SourceSerializer(Serializer):
    def serialize(self, value: Any) -> bytes:
        return value.encode()

    def deserialize(self, value: bytes) -> Any:
        return value.decode()

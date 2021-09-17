import os
import pathlib
from typing import Any

import prefect
from prefect.engine.result import Result
from prefect.engine.serializers import Serializer

from openlineage.prefect.util import get_fs


class MemoryResult(Result):
    def __init__(
        self,
        value: Any = None,
        location: str = None,
        serializer: Serializer = None,
    ):
        super().__init__(value=value, location=location, serializer=serializer)
        self.fs, self.root = get_fs(os.environ.get("FS_URL", "memory:///"))
        self.logger = prefect.context["logger"]

    def _check_parent(self, path):
        """Ensure parent directory exists before we try and write to it"""
        parent = str(pathlib.Path(path).parent)

        if not self.fs.exists(parent):
            self.fs.mkdir(parent)

    def read(self, location: str) -> "Result":
        self.logger.info(f"reading from {location=}")
        new = self.copy()
        new.location = location

        with self.fs.open(f"{self.root}{location}", "rb") as f:
            serialized = f.read()
        new.value = self.serializer.deserialize(serialized)
        self.logger.info(f"Got value {new.value=}")
        return new

    def write(self, value_: Any, **kwargs: Any) -> "Result":
        new = self.format(**kwargs)

        new.value = value_
        value = self.serializer.serialize(new.value)
        self.logger.info(f"Writing to cache location {new.location=} {new.value}")

        fn = f"{self.root}{new.location}"
        self._check_parent(fn)

        with self.fs.open(fn, "wb") as f:
            f.write(value)

        return new

    def exists(self, location: str, **kwargs: Any) -> bool:
        if "task_hash_name" not in kwargs:
            return False
        path = f"{self.root}{location.format(**kwargs)}"
        exists = self.fs.exists(path)
        self.logger.info(f"Checking exists {path=} {exists=}")
        return exists

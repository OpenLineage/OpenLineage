# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
# Copyright 2018-2023 contributors to the Marquez project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from openlineage.client.transport.transport import Config, Transport

if TYPE_CHECKING:
    from openlineage.client.run import DatasetEvent, JobEvent, RunEvent
from openlineage.client.serde import Serde


@dataclass
class FileConfig(Config):
    log_file_path: str
    append: bool = False

    @classmethod
    def from_dict(cls, params: dict[str, Any]) -> FileConfig:
        if "log_file_path" not in params:
            msg = "`log_file_path` key not passed to FileConfig"
            raise RuntimeError(msg)

        log_file_path = params["log_file_path"]

        with open(log_file_path, "a" if cls.append else "w") as log_file_handle:
            if not log_file_handle.writable():
                msg = f"Log file {log_file_path} is not writeable"
                raise RuntimeError(msg)

        return cls(log_file_path)


class FileTransport(Transport):
    kind = "file"
    config_class = FileConfig

    def __init__(self, config: FileConfig) -> None:
        self.config = config

    def emit(self, event: RunEvent | DatasetEvent | JobEvent) -> None:
        if self.config.append:
            log_file_path = self.config.log_file_path
        else:
            timestr = datetime.now().strftime("%Y%m%d-%H%M%S.%f")
            log_file_path = f"{self.config.log_file_path}-{timestr}"

        with open(log_file_path, "a" if self.config.append else "w") as log_file_handle:
            log_file_handle.write(Serde.to_json(event) + "\n")

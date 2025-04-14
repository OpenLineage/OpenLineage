# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Config, Transport

if TYPE_CHECKING:
    from openlineage.client.client import Event

log = logging.getLogger(__name__)


@dataclass
class FileConfig(Config):
    log_file_path: str
    append: bool = False

    @classmethod
    def from_dict(cls, params: dict[str, Any]) -> FileConfig:
        if "log_file_path" not in params:
            msg = "`log_file_path` key not passed to FileConfig"
            raise RuntimeError(msg)

        return cls(log_file_path=params["log_file_path"], append=params.get("append", False))


class FileTransport(Transport):
    kind = "file"
    config_class = FileConfig

    def __init__(self, config: FileConfig) -> None:
        self.config = config
        log.debug(
            "Constructing OpenLineage transport that will send events "
            "to file(s) using the following config: %s",
            self.config,
        )

    def emit(self, event: Event) -> None:
        if self.config.append:
            log_file_path = self.config.log_file_path
        else:
            time_str = datetime.now().strftime("%Y%m%d-%H%M%S.%f")
            log_file_path = f"{self.config.log_file_path}-{time_str}.json"

        log.debug("Openlineage event will be emitted to file: `%s`", log_file_path)
        try:
            with open(log_file_path, "a" if self.config.append else "w") as log_file_handle:
                log_file_handle.write(Serde.to_json(event) + "\n")
        except (PermissionError, io.UnsupportedOperation) as error:
            # If we lack write permissions or file is opened in wrong mode
            msg = f"Log file `{log_file_path}` is not writeable"
            raise RuntimeError(msg) from error

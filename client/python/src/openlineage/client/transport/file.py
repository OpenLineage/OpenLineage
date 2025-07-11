# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import inspect
import io
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Config, Transport
from openlineage.client.utils import import_from_string

if TYPE_CHECKING:
    from openlineage.client.client import Event

log = logging.getLogger(__name__)

# Try to import fsspec for file-like transport support
try:
    import fsspec  # type: ignore[import-not-found]

    FSSPEC_AVAILABLE = True
except ImportError:
    FSSPEC_AVAILABLE = False


@dataclass
class FileConfig(Config):
    """Configuration for file-based OpenLineage event transport.

    Supports local files and remote filesystems via fsspec (optional dependency).
    The transport auto-detects protocols from URL schemes (s3://, gs://, etc.).

    Attributes:
        log_file_path (str): Path to the log file. Can be local or URL with protocol.
        append (bool): Whether to append to existing file (default: False, creates timestamped files).
            Note: Many cloud/object stores (e.g. GCS) do not support true append mode via fsspec.
            In such cases, opening with append may raise an error or be emulated by rewriting the object.
        storage_options (dict): Options passed to the filesystem (e.g., credentials).
        filesystem (str): Optional filesystem provider specification:
            - Class path (e.g., "s3fs.S3FileSystem")
            - Factory function path (e.g., "mymodule.get_filesystem")
            - Instance attribute path (e.g., "mymodule.fs_instance")
        fs_kwargs (dict): Keyword arguments for constructing/calling the filesystem provider.
    """

    log_file_path: str
    append: bool = False
    storage_options: dict[str, Any] | None = None
    # Unified: explicit filesystem provider (class, callable factory, or instance attribute),
    # dotted import path
    filesystem: str | None = None
    # Keyword arguments for constructing/calling the filesystem provider
    fs_kwargs: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, params: dict[str, Any]) -> FileConfig:
        if "log_file_path" not in params:
            msg = "`log_file_path` key not passed to FileConfig"
            raise RuntimeError(msg)

        config = cls(
            log_file_path=params["log_file_path"],
            append=params.get("append", False),
            storage_options=params.get("storage_options"),
            filesystem=params.get("filesystem"),
            fs_kwargs=params.get("fs_kwargs"),
        )

        # Validate configuration
        config._validate()
        return config

    def _validate(self) -> None:
        """Validate configuration and check fsspec availability if needed."""
        # Check if fsspec features are requested but not available
        if self._needs_fsspec() and not FSSPEC_AVAILABLE:
            msg = (
                "An explicit filesystem was requested or remote protocol detected but fsspec is not "
                "available. Please install fsspec to use remote filesystems."
            )
            raise RuntimeError(msg)

        # Warn about conflicting configurations
        if self.filesystem and self.storage_options:
            log.warning("'storage_options' ignored when using explicit filesystem. Use fs_kwargs instead.")

    def _needs_fsspec(self) -> bool:
        """Determine if fsspec is needed based on configuration."""
        # Need fsspec if explicit filesystem is configured
        if self.filesystem:
            return True

        # Need fsspec if the path contains a remote protocol
        return "://" in self.log_file_path and not self.log_file_path.startswith("file://")


class FileHandler(ABC):
    """Abstract base class for file handling implementations."""

    @abstractmethod
    def open_file(self, file_path: str, mode: str) -> Any:
        """Open a file-like handle."""
        pass


class LocalFileHandler(FileHandler):
    """Handler for local filesystem operations."""

    def open_file(self, file_path: str, mode: str) -> Any:
        """Open a file using built-in open for local filesystem."""
        return open(file_path, mode)


class FsspecFileHandler(FileHandler):
    """Handler for fsspec-based filesystem operations."""

    def __init__(self, config: FileConfig) -> None:
        self.config = config
        self._fs = self._create_filesystem()

    def _create_filesystem(self) -> Any | None:
        """Create and return an fsspec filesystem instance if configured, else None."""
        if self.config.filesystem:
            target = import_from_string(self.config.filesystem)
            kwargs = self.config.fs_kwargs or {}
            # If it's a class, instantiate. If it's a callable factory, call it. If it's an instance, use it.
            if inspect.isclass(target):
                fs = target(**kwargs)
            elif callable(target):
                fs = target(**kwargs)
            else:
                fs = target
            if not hasattr(fs, "open"):
                raise RuntimeError(
                    f"'{self.config.filesystem}' did not produce a filesystem implementing an 'open' method"
                )
            return fs
        # Fall back to protocol-based lazy opening (handled in open_file)
        return None

    def open_file(self, file_path: str, mode: str) -> Any:
        """Open a file-like handle using fsspec.

        - If an explicit filesystem instance is configured, use its .open.
        - Else use fsspec.open with auto-detection from URL.
        """
        if self._fs is not None:
            return self._fs.open(file_path, mode)

        storage_options = self.config.storage_options or {}
        try:
            # Let fsspec auto-detect protocol from URL (e.g., s3://, gs://, etc.)
            return fsspec.open(file_path, mode=mode, **storage_options)
        except (ValueError, ImportError) as e:
            # If fsspec can't handle the URL (e.g., local path with no protocol),
            # fall back to built-in open
            log.debug("fsspec could not handle path '%s', falling back to built-in open: %s", file_path, e)
            return open(file_path, mode)


class FileTransport(Transport):
    """File-based transport for OpenLineage events.

    Writes events to local or remote files. Supports automatic protocol detection
    for remote filesystems via fsspec (s3://, gs://, az://, etc.).

    Three filesystem configuration approaches:
    1. Auto-detection: Specify only log_file_path with protocol URL
    2. Explicit filesystem: Use 'filesystem' parameter with class/factory/instance
    3. Storage options: Use 'storage_options' for credentials and configuration
    """

    kind = "file"
    config_class = FileConfig

    def __init__(self, config: FileConfig) -> None:
        self.config = config
        self._file_handler = self._create_file_handler()
        log.debug(
            "Constructing OpenLineage transport that will send events "
            "to file(s) using the following config: %s",
            self.config,
        )

    def _create_file_handler(self) -> FileHandler:
        """Create the appropriate file handler based on configuration and availability."""
        if self.config._needs_fsspec() and FSSPEC_AVAILABLE:
            return FsspecFileHandler(self.config)
        else:
            return LocalFileHandler()

    def _get_file_path(self) -> str:
        """Get the file path, adding timestamp if not in append mode."""
        if self.config.append:
            return self.config.log_file_path
        else:
            time_str = datetime.now().strftime("%Y%m%d-%H%M%S.%f")
            return f"{self.config.log_file_path}-{time_str}.json"

    def emit(self, event: Event) -> None:
        log_file_path = self._get_file_path()
        mode = "a" if self.config.append else "w"

        log.debug("Openlineage event will be emitted to file: `%s`", log_file_path)

        try:
            with self._file_handler.open_file(log_file_path, mode) as f:
                f.write(Serde.to_json(event) + "\n")
        except (PermissionError, io.UnsupportedOperation) as error:
            # If we lack write permissions or file is opened in wrong mode
            msg = f"Log file `{log_file_path}` is not writeable"
            raise RuntimeError(msg) from error
        except Exception as error:
            # Handle fsspec-specific errors or other filesystem errors
            msg = f"Failed to write to log file `{log_file_path}`: {error}"
            raise RuntimeError(msg) from error

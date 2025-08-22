# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import datetime
import json
import os
import tempfile
import time
from os import listdir
from os.path import exists, isfile, join
from typing import Any
from unittest import mock

import pytest
from openlineage.client import OpenLineageClient
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.client.transport.file import FileConfig, FileTransport
from openlineage.client.uuid import generate_new_uuid


def emit_test_events(client: OpenLineageClient, debuff_time: int = 0) -> list[dict[str, Any]]:
    test_event_set = [
        {
            "eventType": RunState.START,
            "name": "test",
            "namespace": "file",
            "producer": "prod",
        },
        {
            "eventType": RunState.COMPLETE,
            "name": "test",
            "namespace": "file",
            "producer": "prod",
        },
    ]

    for test_event in test_event_set:
        event = RunEvent(
            eventType=test_event["eventType"],
            eventTime=datetime.datetime.now().isoformat(),
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace=test_event["namespace"], name=test_event["name"]),
            producer=test_event["producer"],
        )

        client.emit(event)
        time.sleep(debuff_time)  # avoid writing on the same timestamp

    return test_event_set


def assert_test_events(log_line: list[dict[str, Any]], test_event: list[dict[str, Any]]) -> None:
    assert log_line["eventType"] == test_event["eventType"].name
    assert log_line["job"]["name"] == test_event["name"]
    assert log_line["job"]["namespace"] == test_event["namespace"]
    assert log_line["producer"] == test_event["producer"]


def test_client_with_file_transport_append_emits() -> None:
    log_file = tempfile.NamedTemporaryFile()
    config = FileConfig(append=True, log_file_path=log_file.name)
    transport = FileTransport(config)

    client = OpenLineageClient(transport=transport)

    test_event_set = emit_test_events(client)

    with open(log_file.name) as log_file_handle:
        log_file_handle.seek(0)
        log_lines = log_file_handle.read().splitlines()

        for i, raw_log_line in enumerate(log_lines):
            log_line = json.loads(raw_log_line)
            test_event = test_event_set[i]
            assert_test_events(log_line, test_event)


def test_client_with_file_transport_write_emits() -> None:
    log_dir = tempfile.TemporaryDirectory()

    config = FileConfig(log_file_path=join(log_dir.name, "logtest"))
    transport = FileTransport(config)

    client = OpenLineageClient(transport=transport)

    test_event_set = emit_test_events(client, 0.01)

    log_files = [join(log_dir.name, f) for f in listdir(log_dir.name) if isfile(join(log_dir.name, f))]
    log_files.sort()

    for i, log_file in enumerate(log_files):
        with open(log_file) as log_file_handle:
            log_line = json.loads(log_file_handle.read())
            test_event = test_event_set[i]

            assert_test_events(log_line, test_event)


@pytest.mark.parametrize(
    "append",
    [
        True,
        False,
    ],
)
@mock.patch("openlineage.client.transport.file.datetime")
def test_file_transport_raises_error_with_non_writeable_file(mock_dt, append) -> None:
    event = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.datetime.now().isoformat(),
        run=Run(runId=str(generate_new_uuid())),
        job=Job(namespace="file", name="test"),
        producer="prod",
    )
    mock_dt.now().strftime.return_value = "timestr"

    with tempfile.TemporaryDirectory() as log_dir:
        log_base_file_path = join(log_dir, "logtest")

        config = FileConfig(log_file_path=log_base_file_path, append=append)
        transport = FileTransport(config)

        log_file_path = log_base_file_path if append else f"{log_base_file_path}-timestr.json"

        # Create the file and make it read-only
        with open(log_file_path, "w") as f:
            f.write("This is a test file.")
        os.chmod(log_file_path, 0o444)

        with pytest.raises(RuntimeError, match=f"Log file `{log_file_path}` is not writeable"):
            transport.emit(event)

        os.remove(log_file_path)


@mock.patch("openlineage.client.transport.file.datetime")
def test_file_transport_file_path_has_valid_extension_when_not_in_append_mode(mock_dt) -> None:
    event = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.datetime.now().isoformat(),
        run=Run(runId=str(generate_new_uuid())),
        job=Job(namespace="file", name="test"),
        producer="prod",
    )
    mock_dt.now().strftime.return_value = "timestr"

    with tempfile.TemporaryDirectory() as log_dir:
        log_base_file_path = join(log_dir, "logtest")

        config = FileConfig(log_file_path=log_base_file_path, append=False)
        transport = FileTransport(config)

        log_file_path = f"{log_base_file_path}-timestr.json"

        assert not os.path.exists(log_file_path)
        transport.emit(event)
        assert os.path.exists(log_file_path)

        os.remove(log_file_path)


@pytest.mark.parametrize(
    "append",
    [
        True,
        False,
    ],
)
def test_file_config_from_dict(append) -> None:
    log_dir = tempfile.TemporaryDirectory()
    file_path = join(log_dir.name, "logtest")

    config = FileConfig.from_dict(params={"log_file_path": file_path, "append": append})
    assert config.append is append
    assert config.log_file_path == file_path


def test_file_config_from_dict_no_file() -> None:
    with pytest.raises(RuntimeError):
        FileConfig.from_dict({})


@pytest.mark.parametrize(
    "append",
    [
        True,
        False,
    ],
)
def test_file_config_does_not_create_file_if_missing(append) -> None:
    log_dir = tempfile.TemporaryDirectory()
    file_path = join(log_dir.name, "test")

    assert not exists(file_path)
    FileConfig.from_dict(params={"log_file_path": file_path, "append": append})
    assert not exists(file_path)


@pytest.mark.parametrize(
    "append",
    [
        True,
        False,
    ],
)
def test_file_config_append_mode_does_not_modify_file_if_exists(append) -> None:
    log_dir = tempfile.TemporaryDirectory()
    file_path = join(log_dir.name, "test-file")

    with open(file_path, "w") as file:
        file.write("test content")

    assert exists(file_path)
    FileConfig.from_dict(params={"log_file_path": file_path, "append": append})
    assert exists(file_path)

    with open(file_path) as file:
        file_content = file.read()
    assert file_content == "test content"

    os.remove(file_path)


# ======== FSSpec Tests ========


class MockFileSystem:
    """Mock filesystem for testing fsspec functionality."""

    def __init__(self, **kwargs):
        self.storage_options = kwargs
        self._files = {}
        self._open_calls = []

    def open(self, path, mode="r", **kwargs):
        """Mock open method that tracks calls and returns a file-like object."""
        self._open_calls.append((path, mode, kwargs))
        if mode.startswith("w"):
            # For write mode, return a mock file that tracks writes
            return MockFile(path, mode, self._files)
        elif mode.startswith("a"):
            # For append mode, return a mock file that tracks appends
            return MockFile(path, mode, self._files)
        else:
            # For read mode, return content if exists
            if path in self._files:
                return MockFile(path, mode, self._files, initial_content=self._files[path])
            else:
                raise FileNotFoundError(f"File {path} not found")


class MockFile:
    """Mock file-like object for testing."""

    def __init__(self, path, mode, files_dict, initial_content=""):
        self.path = path
        self.mode = mode
        self._files = files_dict
        self._content = initial_content
        self._closed = False

    def write(self, content):
        """Write content to the mock file."""
        if self._closed:
            raise ValueError("I/O operation on closed file")
        if self.mode.startswith("a"):
            self._files[self.path] = self._files.get(self.path, "") + content
        else:
            self._files[self.path] = content

    def read(self):
        """Read content from the mock file."""
        if self._closed:
            raise ValueError("I/O operation on closed file")
        return self._files.get(self.path, "")

    def close(self):
        """Close the mock file."""
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def test_file_config_from_dict_with_fsspec_options():
    """Test FileConfig creation with fsspec-related options."""
    log_dir = tempfile.TemporaryDirectory()
    file_path = join(log_dir.name, "logtest")

    # Test with storage_options only
    config = FileConfig.from_dict(
        params={
            "log_file_path": file_path,
            "append": True,
            "storage_options": {"key": "value", "endpoint_url": "https://minio:9000"},
        }
    )

    assert config.log_file_path == file_path
    assert config.append is True
    assert config.storage_options == {"key": "value", "endpoint_url": "https://minio:9000"}
    assert config.filesystem is None
    assert config.fs_kwargs is None


def test_file_config_from_dict_with_filesystem():
    """Test FileConfig creation with explicit filesystem configuration."""
    log_dir = tempfile.TemporaryDirectory()
    file_path = join(log_dir.name, "logtest")

    # Test with filesystem and fs_kwargs
    config = FileConfig.from_dict(
        params={
            "log_file_path": file_path,
            "filesystem": "tests.test_file.MockFileSystem",
            "fs_kwargs": {"endpoint_url": "https://minio:9000", "region": "us-east-1"},
        }
    )

    assert config.log_file_path == file_path
    assert config.append is False  # default
    assert config.storage_options is None
    assert config.filesystem == "tests.test_file.MockFileSystem"
    assert config.fs_kwargs == {"endpoint_url": "https://minio:9000", "region": "us-east-1"}


@mock.patch("openlineage.client.transport.file.FSSPEC_AVAILABLE", False)
def test_file_transport_raises_error_when_fsspec_not_available_with_filesystem():
    """Test that FileConfig raises error when fsspec is not available but filesystem is specified."""
    log_dir = tempfile.TemporaryDirectory()
    file_path = join(log_dir.name, "logtest")

    with pytest.raises(RuntimeError):
        FileConfig.from_dict({"log_file_path": file_path, "filesystem": "tests.test_file.MockFileSystem"})


@mock.patch("openlineage.client.transport.file.FSSPEC_AVAILABLE", True)
def test_file_transport_creates_filesystem_from_class():
    """Test that FileTransport can create filesystem from a class."""
    log_dir = tempfile.TemporaryDirectory()
    file_path = join(log_dir.name, "logtest")

    config = FileConfig(
        log_file_path=file_path,
        filesystem="tests.test_file.MockFileSystem",
        fs_kwargs={"test_arg": "test_value"},
    )

    transport = FileTransport(config)
    assert hasattr(transport, "_file_handler")
    assert hasattr(transport._file_handler, "_fs")
    assert transport._file_handler._fs is not None
    assert isinstance(transport._file_handler._fs, MockFileSystem)
    assert transport._file_handler._fs.storage_options == {"test_arg": "test_value"}


@mock.patch("openlineage.client.transport.file.FSSPEC_AVAILABLE", True)
def test_file_transport_creates_filesystem_from_callable():
    """Test that FileTransport can create filesystem from a callable factory."""

    def mock_filesystem_factory(**kwargs):
        return MockFileSystem(**kwargs)

    log_dir = tempfile.TemporaryDirectory()
    file_path = join(log_dir.name, "logtest")

    # We'll mock the import_from_string to return our factory function
    with mock.patch(
        "openlineage.client.transport.file.import_from_string", return_value=mock_filesystem_factory
    ):
        config = FileConfig(
            log_file_path=file_path,
            filesystem="some.factory.function",
            fs_kwargs={"factory_arg": "factory_value"},
        )

        transport = FileTransport(config)
        assert hasattr(transport, "_file_handler")
        assert hasattr(transport._file_handler, "_fs")
        assert transport._file_handler._fs is not None
        assert isinstance(transport._file_handler._fs, MockFileSystem)
        assert transport._file_handler._fs.storage_options == {"factory_arg": "factory_value"}


@mock.patch("openlineage.client.transport.file.FSSPEC_AVAILABLE", True)
def test_file_transport_uses_existing_filesystem_instance():
    """Test that FileTransport can use an existing filesystem instance."""
    mock_fs = MockFileSystem(existing="instance")

    log_dir = tempfile.TemporaryDirectory()
    file_path = join(log_dir.name, "logtest")

    # Mock import_from_string to return the instance directly
    with mock.patch("openlineage.client.transport.file.import_from_string", return_value=mock_fs):
        config = FileConfig(
            log_file_path=file_path, filesystem="some.existing.instance", fs_kwargs={"should": "be_ignored"}
        )

        transport = FileTransport(config)
        assert hasattr(transport, "_file_handler")
        assert hasattr(transport._file_handler, "_fs")
        assert transport._file_handler._fs is mock_fs
        assert transport._file_handler._fs.storage_options == {"existing": "instance"}


@mock.patch("openlineage.client.transport.file.FSSPEC_AVAILABLE", True)
def test_file_transport_raises_error_for_invalid_filesystem():
    """Test that FileTransport raises error when filesystem doesn't have open method."""

    class InvalidFileSystem:
        """Filesystem without open method."""

        pass

    log_dir = tempfile.TemporaryDirectory()
    file_path = join(log_dir.name, "logtest")

    with mock.patch("openlineage.client.transport.file.import_from_string", return_value=InvalidFileSystem):
        config = FileConfig(log_file_path=file_path, filesystem="some.invalid.filesystem")

        with pytest.raises(RuntimeError, match="did not produce a filesystem implementing an 'open' method"):
            FileTransport(config)


@mock.patch("openlineage.client.transport.file.FSSPEC_AVAILABLE", True)
def test_file_transport_emit_with_explicit_filesystem():
    """Test that FileTransport.emit works with explicit filesystem."""
    log_dir = tempfile.TemporaryDirectory()
    file_path = join(log_dir.name, "logtest")

    config = FileConfig(log_file_path=file_path, filesystem="tests.test_file.MockFileSystem", append=True)

    transport = FileTransport(config)

    event = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.datetime.now().isoformat(),
        run=Run(runId=str(generate_new_uuid())),
        job=Job(namespace="file", name="test"),
        producer="prod",
    )

    transport.emit(event)

    # Verify that the mock filesystem was used
    mock_fs = transport._file_handler._fs
    assert len(mock_fs._open_calls) == 1
    assert mock_fs._open_calls[0][0] == file_path  # path
    assert mock_fs._open_calls[0][1] == "a"  # mode (append)

    # Verify that content was written
    assert file_path in mock_fs._files
    written_content = mock_fs._files[file_path]
    assert "START" in written_content
    assert "file" in written_content  # namespace
    assert "test" in written_content  # job name


@mock.patch("openlineage.client.transport.file.FSSPEC_AVAILABLE", True)
def test_file_transport_emit_handles_fsspec_errors():
    """Test that FileTransport.emit handles fsspec-specific errors properly."""
    log_dir = tempfile.TemporaryDirectory()
    file_path = join(log_dir.name, "logtest")

    # Create a filesystem that raises an error on open
    class ErrorFileSystem:
        def open(self, path, mode="r", **kwargs):
            raise OSError("Network connection failed")

    with mock.patch("openlineage.client.transport.file.import_from_string", return_value=ErrorFileSystem):
        config = FileConfig(log_file_path=file_path, filesystem="some.error.filesystem")

        transport = FileTransport(config)

        event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.datetime.now().isoformat(),
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="file", name="test"),
            producer="prod",
        )

        with pytest.raises(
            RuntimeError,
            match=f"Failed to write to log file `{file_path}-.*\\.json`: Network connection failed",
        ):
            transport.emit(event)


@mock.patch("openlineage.client.transport.file.datetime")
def test_file_transport_get_file_path_with_timestamp(mock_dt):
    """Test that _get_file_path generates correct timestamped paths when not in append mode."""
    mock_dt.now().strftime.return_value = "20230815-143052.123456"

    log_dir = tempfile.TemporaryDirectory()
    base_path = join(log_dir.name, "logtest")

    config = FileConfig(log_file_path=base_path, append=False)
    transport = FileTransport(config)

    file_path = transport._get_file_path()
    assert file_path == f"{base_path}-20230815-143052.123456.json"


def test_file_transport_get_file_path_with_append():
    """Test that _get_file_path returns original path when in append mode."""
    log_dir = tempfile.TemporaryDirectory()
    base_path = join(log_dir.name, "logtest")

    config = FileConfig(log_file_path=base_path, append=True)
    transport = FileTransport(config)

    file_path = transport._get_file_path()
    assert file_path == base_path


@mock.patch("openlineage.client.transport.file.FSSPEC_AVAILABLE", True)
def test_file_transport_open_file_local_fallback():
    """Test that file handler falls back to built-in open when no fsspec configuration is provided."""
    log_dir = tempfile.TemporaryDirectory()
    file_path = join(log_dir.name, "logtest")

    config = FileConfig(log_file_path=file_path)
    transport = FileTransport(config)

    # Should use built-in open since no fsspec configuration
    with transport._file_handler.open_file(file_path, "w") as f:
        f.write("test content")

    # Verify file was created using standard filesystem
    assert os.path.exists(file_path)
    with open(file_path, "r") as f:
        assert f.read() == "test content"


@mock.patch("openlineage.client.transport.file.FSSPEC_AVAILABLE", True)
@mock.patch("openlineage.client.transport.file.fsspec")
def test_file_transport_protocol_auto_detection(mock_fsspec):
    """Test that file handler auto-detects protocol from URL scheme."""
    s3_path = "s3://my-bucket/lineage/events.jsonl"

    # Mock fsspec.open to return our mock file
    mock_file = MockFile(s3_path, "w", {})
    mock_fsspec.open.return_value.__enter__.return_value = mock_file
    mock_fsspec.open.return_value.__exit__.return_value = None

    # Protocol will be auto-detected from s3:// scheme
    config = FileConfig(log_file_path=s3_path, storage_options={"key": "value"})
    transport = FileTransport(config)

    # Test opening file with auto-detection
    with transport._file_handler.open_file(s3_path, "w") as f:
        f.write("test content")

    # Verify that fsspec.open was called without protocol parameter (auto-detection)
    mock_fsspec.open.assert_called_once_with(s3_path, mode="w", key="value")


@mock.patch("openlineage.client.transport.file.FSSPEC_AVAILABLE", True)
@mock.patch("openlineage.client.transport.file.fsspec")
def test_file_transport_uses_local_handler_for_local_paths(mock_fsspec):
    """Test that file transport uses LocalFileHandler for local paths without protocol."""
    log_dir = tempfile.TemporaryDirectory()
    local_path = join(log_dir.name, "logtest")

    config = FileConfig(log_file_path=local_path)
    transport = FileTransport(config)

    # Should use LocalFileHandler, not fsspec
    from openlineage.client.transport.file import LocalFileHandler

    assert isinstance(transport._file_handler, LocalFileHandler)

    # Verify file can be opened and written
    with transport._file_handler.open_file(local_path, "w") as f:
        f.write("test content")

    # Verify fsspec was never called since we use LocalFileHandler
    mock_fsspec.open.assert_not_called()

    # Verify file was created using standard filesystem
    assert os.path.exists(local_path)
    with open(local_path, "r") as f:
        assert f.read() == "test content"

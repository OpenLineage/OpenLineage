# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List

import docker
import pytest
import requests


@pytest.fixture(scope="session")
def docker_client():
    """Docker client for managing containers."""
    return docker.from_env()


@pytest.fixture(scope="session")
def docker_compose_project():
    """Start docker-compose services and clean up after tests."""
    test_dir = Path(__file__).parent
    wheels_dir = test_dir / "wheels"

    # Create wheels directory
    wheels_dir.mkdir(exist_ok=True)

    # Clean up any existing wheels
    for wheel in wheels_dir.glob("*.whl"):
        wheel.unlink()

    # Build wheels for OpenLineage components
    openlineage_root = test_dir.parent.parent.parent.parent

    # Build client/python wheel
    client_python_dir = openlineage_root / "client" / "python"
    print(f"Building client python wheel from {client_python_dir}")
    subprocess.run(
        [
            "bash",
            "-c",
            f"cd {client_python_dir} && rm -rf dist/* && "
            f"uv venv && "
            f"source .venv/bin/activate && "
            f"uv sync && "
            f"uv run python -m build --wheel && "
            f"cp dist/* {wheels_dir}/",
        ],
        check=True,
        shell=False,
    )

    # Build integration/sql/iface-py wheel using Docker (if not already built)
    integration_sql_dir = openlineage_root / "integration" / "sql"
    sql_iface_py_dir = integration_sql_dir / "iface-py"
    sql_dist_dir = sql_iface_py_dir / "dist"

    # Read current version from dbt pyproject.toml
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib

    dbt_pyproject_path = openlineage_root / "integration" / "dbt" / "pyproject.toml"
    with open(dbt_pyproject_path, "rb") as f:
        dbt_config = tomllib.load(f)
    current_version = dbt_config["project"]["version"]

    # Check if there's already a valid SQL wheel with matching version and platform
    # Docker containers run Linux, so we need a Linux-compatible wheel
    if sql_dist_dir.exists():
        all_sql_wheels = list(sql_dist_dir.glob(f"openlineage_sql-{current_version}-*.whl"))
        # Filter for Linux-compatible wheels for Docker usage
        existing_sql_wheels = [w for w in all_sql_wheels if "linux" in w.name.lower()]
    else:
        existing_sql_wheels = []

    if existing_sql_wheels:
        print(f"Using existing SQL wheel: {existing_sql_wheels[0]}")
        subprocess.run(["cp", str(existing_sql_wheels[0]), str(wheels_dir)], check=True)
    else:
        print(f"Building integration sql wheel from {integration_sql_dir}")
        dockerfile_path = integration_sql_dir / "Dockerfile.python"
        subprocess.run(
            [
                "docker",
                "build",
                "-t",
                "openlineage-sql-builder",
                "-f",
                str(dockerfile_path),
                str(integration_sql_dir),
            ],
            check=True,
        )
        subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "-v",
                f"{wheels_dir}:/output",
                "openlineage-sql-builder",
                "sh",
                "-c",
                "cp /wheels/* /output/",
            ],
            check=True,
        )

    # Build integration/common wheel
    integration_common_dir = openlineage_root / "integration" / "common"
    print(f"Building integration common wheel from {integration_common_dir}")
    subprocess.run(
        [
            "bash",
            "-c",
            f"cd {integration_common_dir} && rm -rf dist/* && "
            f"uv venv && "
            f"source .venv/bin/activate && "
            f"uv pip install setuptools && "
            f"python setup.py bdist_wheel && "
            f"cp dist/* {wheels_dir}/",
        ],
        check=True,
        shell=False,
    )

    # Build integration/dbt wheel
    integration_dbt_dir = openlineage_root / "integration" / "dbt"
    print(f"Building integration dbt wheel from {integration_dbt_dir}")
    subprocess.run(
        [
            "bash",
            "-c",
            f"cd {integration_dbt_dir} && "
            f"rm -rf dist/* && "
            f"uv venv && "
            f"source .venv/bin/activate && "
            f"uv pip install setuptools && "
            f"python setup.py bdist_wheel && "
            f"cp dist/* {wheels_dir}/",
        ],
        check=True,
        shell=False,
    )

    print(f"Built wheels: {list(wheels_dir.glob('*.whl'))}")

    subprocess.run(["docker-compose", "up", "-d", "--build", "--remove-orphans"], check=True, cwd=test_dir)
    _wait_for_test_server()

    yield

    subprocess.run(["docker-compose", "down", "-v"], cwd=test_dir)


@pytest.fixture(scope="session")
def test_server_url(docker_compose_project):
    return "http://localhost:8089"


@pytest.fixture(scope="session")
def dbt_container(docker_client, docker_compose_project):
    """Get reference to dbt container."""
    containers = docker_client.containers.list(filters={"label": "com.docker.compose.service=dbt-runner"})
    if not containers:
        container_name = "dbt-integration_dbt-runner_1"
        try:
            container = docker_client.containers.get(container_name)
            return container
        except docker.errors.NotFound:
            pass

        container_name = "integration_dbt-runner_1"
        try:
            container = docker_client.containers.get(container_name)
            return container
        except docker.errors.NotFound:
            pass

        raise RuntimeError("Could not find dbt-runner container")

    return containers[0]


@pytest.fixture
def reset_test_server(test_server_url):
    """Reset test server state before each test."""
    response = requests.post(f"{test_server_url}/reset")
    response.raise_for_status()
    yield


@pytest.fixture
def dbt_runner(dbt_container, test_server_url):
    """Helper for running dbt commands in container."""

    class DbtRunner:
        def __init__(self, container, server_url):
            self.container = container
            self.server_url = server_url

        def run_dbt_command(self, args: List[str], expect_failure=False) -> Dict[str, Any]:
            """Run dbt command in container and return result."""
            cmd = ["dbt"] + args

            internal_server_url = "http://test-server:8080"

            result = self.container.exec_run(
                cmd,
                environment={
                    "OPENLINEAGE_URL": internal_server_url,
                    "OPENLINEAGE_NAMESPACE": "dbt_integration_test",
                },
            )

            success = result.exit_code == 0
            output = result.output.decode()

            if not expect_failure and not success:
                raise RuntimeError(f"dbt command failed: {output}")

            return {"exit_code": result.exit_code, "output": output, "success": success}

        def run_dbt_ol_command(self, args: List[str], expect_failure=False) -> Dict[str, Any]:
            """Run dbt-ol command in container."""
            cmd = ["dbt-ol"] + args

            internal_server_url = "http://test-server:8080"

            result = self.container.exec_run(
                cmd,
                environment={
                    "OPENLINEAGE_URL": internal_server_url,
                    "OPENLINEAGE_NAMESPACE": "dbt_integration_test",
                },
            )

            success = result.exit_code == 0
            output = result.output.decode()

            if not expect_failure and not success:
                raise RuntimeError(f"dbt command failed: {output}")

            return {"exit_code": result.exit_code, "output": output, "success": success}

        def get_events(self) -> List[Dict[str, Any]]:
            """Get events from test server."""
            response = requests.get(f"{self.server_url}/events")
            response.raise_for_status()
            events_data = response.json()

            if isinstance(events_data, list):
                if events_data and isinstance(events_data[0], dict):
                    if "payload" in events_data[0]:
                        return [event["payload"] for event in events_data]
                    else:
                        return events_data
            return []

        def get_events_for_job(self, job_name: str) -> List[Dict[str, Any]]:
            response = requests.get(f"{self.server_url}/events", params={"job_name": job_name})
            response.raise_for_status()
            events_data = response.json()

            if isinstance(events_data, list):
                if events_data and isinstance(events_data[0], dict):
                    if "payload" in events_data[0]:
                        return [event["payload"] for event in events_data]
                    else:
                        return events_data
            return []

        def get_validation_summary(self) -> Dict[str, Any]:
            """Get validation summary from test server."""
            response = requests.get(f"{self.server_url}/validation/summary")
            response.raise_for_status()
            return response.json()

    return DbtRunner(dbt_container, test_server_url)


def _wait_for_test_server(max_attempts=30):
    """Wait for test server to be ready."""
    for attempt in range(max_attempts):
        try:
            response = requests.get("http://localhost:8089/health", timeout=1)
            if response.status_code == 200:
                print(f"Test server ready after {attempt + 1} attempts")
                return
        except (requests.exceptions.RequestException, requests.exceptions.Timeout):
            pass
        time.sleep(1)
    raise RuntimeError("Test server did not become ready")

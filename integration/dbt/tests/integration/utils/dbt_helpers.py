# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, List, Optional, Tuple


class DbtRunner:
    """Helper class for running dbt commands in a container."""

    def __init__(self, container, project_dir: str = "/opt/dbt"):
        self.container = container
        self.project_dir = project_dir
        self.profiles_dir = project_dir

    def run_command(self, command: List[str], env: Optional[Dict[str, str]] = None) -> Tuple[int, str]:
        """Run a dbt command in the container."""
        full_command = ["dbt"] + command

        # Set working directory and profiles directory
        exec_command = ["bash", "-c", f"cd {self.project_dir} && {' '.join(full_command)}"]

        # Set environment variables
        container_env = {
            "DBT_PROFILES_DIR": self.profiles_dir,
            "DBT_PROJECT_DIR": self.project_dir,
        }
        if env:
            container_env.update(env)

        result = self.container.exec_run(exec_command, environment=container_env, workdir=self.project_dir)

        return result.exit_code, result.output.decode("utf-8")

    def seed(self, select: Optional[str] = None, env: Optional[Dict[str, str]] = None) -> Tuple[int, str]:
        """Run dbt seed command."""
        command = ["seed"]
        if select:
            command.extend(["--select", select])
        return self.run_command(command, env)

    def run(self, select: Optional[str] = None, env: Optional[Dict[str, str]] = None) -> Tuple[int, str]:
        """Run dbt run command."""
        command = ["run"]
        if select:
            command.extend(["--select", select])
        return self.run_command(command, env)

    def test(self, select: Optional[str] = None, env: Optional[Dict[str, str]] = None) -> Tuple[int, str]:
        """Run dbt test command."""
        command = ["test"]
        if select:
            command.extend(["--select", select])
        return self.run_command(command, env)

    def run_full_pipeline(self, env: Optional[Dict[str, str]] = None) -> List[Tuple[str, int, str]]:
        """Run the full dbt pipeline: seed -> run -> test."""
        results = []

        # Run seed
        exit_code, output = self.seed(env=env)
        results.append(("seed", exit_code, output))

        if exit_code != 0:
            return results

        # Run models
        exit_code, output = self.run(env=env)
        results.append(("run", exit_code, output))

        if exit_code != 0:
            return results

        # Run tests
        exit_code, output = self.test(env=env)
        results.append(("test", exit_code, output))

        return results

    def get_openlineage_env(
        self, mode: str = "structured_logs", namespace: str = "dbt_test"
    ) -> Dict[str, str]:
        """Get OpenLineage environment variables for different modes."""
        base_env = {
            "OPENLINEAGE_NAMESPACE": namespace,
            "OPENLINEAGE_URL": "http://test-server:8080",
        }

        if mode == "structured_logs":
            # For structured logs mode, we don't need additional config
            return base_env
        elif mode == "local_artifacts":
            # For local artifacts mode, disable structured logs
            base_env["OPENLINEAGE_DISABLED"] = "false"
            return base_env
        else:
            raise ValueError(f"Unknown mode: {mode}")

    def run_with_openlineage(self, command: List[str], mode: str = "structured_logs") -> Tuple[int, str]:
        """Run dbt command with OpenLineage integration."""
        env = self.get_openlineage_env(mode)

        if mode == "structured_logs":
            # Use dbt-ol wrapper for structured logs
            full_command = ["dbt-ol", "--consume-structured-logs"] + command
            exec_command = ["bash", "-c", f"cd {self.project_dir} && {' '.join(full_command)}"]
        else:
            # Use regular dbt for local artifacts
            full_command = ["dbt"] + command
            exec_command = ["bash", "-c", f"cd {self.project_dir} && {' '.join(full_command)}"]

        container_env = {
            "DBT_PROFILES_DIR": self.profiles_dir,
            "DBT_PROJECT_DIR": self.project_dir,
        }
        container_env.update(env)

        result = self.container.exec_run(exec_command, environment=container_env, workdir=self.project_dir)

        return result.exit_code, result.output.decode("utf-8")

# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import configparser
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
SETUP = ROOT / ".circleci/setup-datadog-tests.sh"
SPARK = ROOT / "integration/spark/cli/configurable-test.sh"


class DatadogSetupTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="datadog tests ")
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name)
        self.bin = self.path / "bin"
        self.bin.mkdir()
        self.env_file = self.path / "bash_env"
        self.env_file.touch()
        self.record = self.path / "calls.json"
        self.env = {
            "PATH": f"{self.bin}:{os.environ['PATH']}",
            "BASH_ENV": str(self.env_file),
            "TMPDIR": str(self.path),
            "TEST_RECORD": str(self.record),
            "DD_API_KEY": "test-optimization-placeholder",
        }
        self.tool(
            "uv",
            "Path(os.environ['TEST_RECORD']).write_text(json.dumps(sys.argv[1:]))\n"
            "sys.exit(int(os.environ.get('INSTALL_EXIT_CODE', '0')))\n",
        )
        self.tool(
            "curl",
            "if os.environ.get('INSTALL_EXIT_CODE'):\n"
            "    sys.exit(int(os.environ['INSTALL_EXIT_CODE']))\n"
            "Path(sys.argv[sys.argv.index('--output') + 1]).write_bytes(b'test agent')\n",
        )

    def tool(self, name, body):
        path = self.bin / name
        path.write_text(
            f"#!{sys.executable}\nimport json, os, sys\nfrom pathlib import Path\n" + body,
        )
        path.chmod(0o755)

    def setup(self, mode="pytest", service="openlineage-test", python=".tox/py314/bin/python"):
        return subprocess.run(
            ["bash", str(SETUP), mode, service, python],
            env=self.env,
            cwd=self.path,
            capture_output=True,
            text=True,
            check=False,
        )

    def configured_environment(self):
        # bash sources BASH_ENV exactly as it does for the following CircleCI step.
        result = subprocess.run(
            [
                "bash",
                "-c",
                'exec "$@"',
                "bash",
                sys.executable,
                "-c",
                "import json, os; print(json.dumps(dict(os.environ)))",
            ],
            env=self.env,
            cwd=self.path,
            capture_output=True,
            text=True,
            check=True,
        )
        return json.loads(result.stdout)

    def test_no_key_does_not_install_or_enable_instrumentation(self):
        self.env.pop("DD_API_KEY")
        for mode in ("pytest", "java-container"):
            with self.subTest(mode=mode):
                result = self.setup(mode)
                self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(self.record.exists())
        self.assertEqual(self.env_file.read_text(), "")
        self.assertEqual(list(self.path.glob("openlineage-datadog.*")), [])

    def test_continuation_parameters_gate_the_orb_without_persisting_credentials(self):
        setup = yaml.safe_load((ROOT / ".circleci/config.yml").read_text())
        steps = setup["jobs"]["determine_changed_modules"]["steps"]
        command = next(
            step["run"]["command"]
            for step in steps
            if isinstance(step, dict)
            and isinstance(step.get("run"), dict)
            and step["run"].get("name") == "Prepare continuation parameters"
        )
        for key in ("", "test-optimization-placeholder"):
            with self.subTest(credentials=bool(key)):
                self.env["DD_API_KEY"] = key
                subprocess.run(
                    ["bash", "-eo", "pipefail", "-c", command],
                    env=self.env,
                    cwd=self.path,
                    check=True,
                )
                for context in ("pr", "release"):
                    data = json.loads((self.path / f"datadog-{context}-parameters.json").read_text())
                    self.assertEqual(data, {"build-context": context, "datadog-enabled": bool(key)})
        config = yaml.safe_load((ROOT / ".circleci/continue_config.yml").read_text())
        self.assertFalse(config["parameters"]["datadog-enabled"]["default"])
        gate = config["commands"]["instrument_tests"]["steps"][0]["when"]
        self.assertEqual(gate["condition"], "<< pipeline.parameters.datadog-enabled >>")
        self.assertIn("datadog/autoinstrument", gate["steps"][0])

    def test_pytest_installs_into_the_requested_interpreter(self):
        result = self.setup(python=".tox/Python 3.14/bin/python")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            json.loads(self.record.read_text()),
            ["pip", "install", "--python", ".tox/Python 3.14/bin/python", "ddtrace"],
        )
        env = self.configured_environment()
        self.assertEqual(env["PYTEST_ADDOPTS"], "--ddtrace")
        self.assertEqual(env["DD_CIVISIBILITY_AGENTLESS_ENABLED"], "true")
        self.assertEqual(env["DD_SITE"], "us5.datadoghq.com")
        self.assertEqual(env["DD_ENV"], "ci")
        self.assertNotIn(self.env["DD_API_KEY"], self.env_file.read_text() + result.stdout)

    def test_setup_preserves_options_and_quotes_environment_values(self):
        self.env.update(DD_SITE="datadoghq.eu", DD_ENV="test ci", PYTEST_ADDOPTS="-q --ddtrace")
        service = "openlineage $(touch injected)"
        result = self.setup(service=service)
        self.assertEqual(result.returncode, 0, result.stderr)
        env = self.configured_environment()
        self.assertEqual(env["PYTEST_ADDOPTS"], "-q --ddtrace")
        self.assertEqual(env["DD_SERVICE"], service)
        self.assertEqual(env["DD_ENV"], "test ci")
        self.assertEqual(env["DD_SITE"], "datadoghq.eu")
        self.assertFalse((self.path / "injected").exists())

    def test_failed_install_does_not_enable_a_missing_tracer(self):
        self.env["INSTALL_EXIT_CODE"] = "23"
        for mode in ("pytest", "java-container"):
            with self.subTest(mode=mode):
                result = self.setup(mode)
                self.assertEqual(result.returncode, 23 if mode == "pytest" else 1)
        self.assertEqual(self.env_file.read_text(), "")
        self.assertEqual(list(self.path.glob("openlineage-datadog.*")), [])

    def spark(self):
        self.tool(
            "docker",
            "record = Path(os.environ['TEST_RECORD'])\n"
            "calls = json.loads(record.read_text()) if record.exists() else []\n"
            "calls.append(sys.argv[1:])\n"
            "record.write_text(json.dumps(calls))\n"
            "if sys.argv[1] == 'images': print('existing-image')\n"
            "if sys.argv[1:3] == ['network', 'ls']: print('openlineage')\n"
            "if sys.argv[1] == 'run': sys.exit(int(os.environ.get('TEST_EXIT_CODE', '0')))\n",
        )
        return subprocess.run(
            [
                "bash",
                str(SPARK),
                "--spark",
                "./integration/spark/cli/spark-conf.yml",
                "--test",
                "./integration/spark/cli/tests",
            ],
            env=self.env,
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_spark_container_receives_the_agent_and_metadata_without_key_in_arguments(self):
        result = self.setup("java-container")
        self.assertEqual(result.returncode, 0, result.stderr)
        env = self.configured_environment()
        agent = Path(env["DD_TEST_JAVA_AGENT"])
        self.assertEqual(agent.read_bytes(), b"test agent")
        result = self.spark()
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = json.loads(self.record.read_text())
        args = calls[-1]
        self.assertEqual(args[0], "run")
        self.assertIn(
            f"type=bind,source={agent},target=/opt/datadog/dd-java-agent.jar,readonly",
            args,
        )
        self.assertIn("JAVA_TOOL_OPTIONS=-javaagent:/opt/datadog/dd-java-agent.jar", args)
        for name in ("DD_API_KEY", "DD_SITE", "DD_SERVICE", "CIRCLE_SHA1", "CIRCLE_WORKFLOW_ID"):
            self.assertIn(name, args)
        self.assertNotIn(self.env["DD_API_KEY"], json.dumps(calls) + result.stdout)

    def test_uninstrumented_spark_keeps_test_failure_exit_status(self):
        self.env.pop("DD_API_KEY")
        self.env["TEST_EXIT_CODE"] = "37"
        result = self.spark()
        self.assertEqual(result.returncode, 37)
        args = json.loads(self.record.read_text())[-1]
        self.assertFalse(any("datadog" in arg or arg.startswith("DD_") for arg in args))

    def test_playwright_bootstrap_is_optional_and_preserves_node_options(self):
        workflow = yaml.safe_load((ROOT / ".github/workflows/visual-difference-detection.yml").read_text())
        self.tool(
            "yarn",
            "Path(os.environ['TEST_RECORD']).write_text(os.environ.get('NODE_OPTIONS', ''))\n",
        )
        for name in ("take-screenshots-main", "take-screenshots-pull-request"):
            step = next(
                s
                for s in workflow["jobs"][name]["steps"]
                if s.get("name") == "Take screenshots with Playwright"
            )
            for package in ("", "/tmp/dd-trace/ci/init"):
                with self.subTest(job=name, package=package):
                    self.env.update(NODE_OPTIONS="--max-old-space-size=4096", DD_TRACE_PACKAGE=package)
                    result = subprocess.run(
                        ["bash", "-eo", "pipefail", "-c", step["run"]],
                        env=self.env,
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    expected = "--max-old-space-size=4096" + (f" -r {package}" if package else "")
                    self.assertEqual(self.record.read_text(), expected)

    def test_main_screenshots_use_the_checkout_commit_instead_of_the_pull_request(self):
        workflow = yaml.safe_load((ROOT / ".github/workflows/visual-difference-detection.yml").read_text())
        steps = workflow["jobs"]["take-screenshots-main"]["steps"]
        command = next(s["run"] for s in steps if s.get("name") == "Record the main checkout for Datadog")
        self.tool("git", "print('a' * 40)\n")
        self.env.update(GITHUB_SHA="b" * 40, GITHUB_ENV=str(self.path / "github_env"))
        subprocess.run(
            ["bash", "-eo", "pipefail", "-c", command],
            env=self.env,
            cwd=self.path,
            check=True,
        )
        self.assertEqual(
            (self.path / "github_env").read_text(),
            "DD_GIT_BRANCH=main\nDD_GIT_COMMIT_SHA=" + "a" * 40 + "\n",
        )


class DatadogConfigurationTests(unittest.TestCase):
    def test_tox_forwards_the_tracer_options_and_ci_metadata(self):
        required = {"DD_*", "CIRCLE*", "CI", "PYTEST_ADDOPTS"}
        for project in ("client/python", "integration/common"):
            text = (ROOT / project / "pyproject.toml").read_text()
            ini = re.search(r'legacy_tox_ini = """(.*?)"""', text, re.DOTALL).group(1)
            parser = configparser.ConfigParser(interpolation=None)
            parser.read_string(ini)
            with self.subTest(project=project):
                self.assertTrue(required.issubset(parser["testenv"]["pass_env"].split()))

    def test_native_setup_precedes_all_direct_test_commands(self):
        config = yaml.safe_load((ROOT / ".circleci/continue_config.yml").read_text())

        def check_steps(job, steps, configured=False):
            for step in steps:
                if not isinstance(step, dict):
                    continue
                if "when" in step:
                    check_steps(job, step["when"]["steps"], configured)
                if "instrument_tests" in step:
                    with self.subTest(job=job):
                        environment = config["jobs"][job].get("environment", {})
                        self.assertTrue(environment.get("DD_SERVICE"), "Test job needs a component service")
                        self.assertEqual(environment.get("DD_ENV"), "ci")
                    configured = True
                if "instrument_pytest" in step:
                    configured = True
                if "run" not in step:
                    continue
                command = step["run"]
                if isinstance(command, dict):
                    command = command["command"]
                if "setup-datadog-tests.sh java-container" in command:
                    configured = True
                test_command = re.sub(r"-x\s+test\b", "", command)
                runs_tests = (
                    re.search(
                        r"gradlew[^\n]*\b(?:build|check|test|integrationTest|databricksIntegrationTest)\b",
                        test_command,
                    )
                    or "bash script/build.sh" in command
                    or "--skip-pkg-install" in command
                    or "pytest tests/" in command
                    or "go test ./..." in command
                    or "cli/configurable-test.sh --spark" in command
                )
                if runs_tests:
                    with self.subTest(job=job, command=command):
                        self.assertTrue(configured, "Test command has no native Datadog setup")

        for job, settings in config["jobs"].items():
            check_steps(job, settings["steps"])


if __name__ == "__main__":
    unittest.main()

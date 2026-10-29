# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import importlib.machinery
import importlib.metadata
import importlib.util
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import sysconfig
import uuid
from datetime import datetime

from openlineage.client.event_v2 import InputDataset, Job, OutputDataset, Run, RunEvent, RunState
from openlineage.common.provider.dbt.facets import ParentRunMetadata
from openlineage.common.utils import get_from_nullable_chain, parse_single_arg

__version__ = "1.56.0"
PRODUCER = f"https://github.com/OpenLineage/OpenLineage/tree/{__version__}/integration/dbt"

# for which command structured logs consumption is implemented
HANDLED_COMMANDS = ["run", "seed", "snapshot", "test", "build", "retry"]
CONSUME_STRUCTURED_LOGS_COMMAND_OPTION = "--consume-structured-logs"
OPENLINEAGE_DBT_JOB_NAME_OPTION = "--openlineage-dbt-job-name"
DBT_LOG_FILE_MAX_BYTES = str(5 * 1024 * 1024 * 1024)
# dbt Core v2: skip the `dbt parse` that runs before the command (the flag is removed before dbt runs)
OL_SKIP_PARSE_OPTION = "--ol-skip-parse"
OL_SKIP_PARSE_ENV = "OPENLINEAGE_DBT_SKIP_PARSE"
# where dbt Core v2 runs create their temporary directory; defaults to the system temp directory
TEMP_DIR_ENV = "OPENLINEAGE_DBT_TEMP_DIR"
# seconds the `dbt parse` before a dbt Core v2 run may take before it's stopped
PARSE_TIMEOUT_ENV = "OPENLINEAGE_DBT_PARSE_TIMEOUT"

log = logging.getLogger(__name__)


def get_event_timestamp(timestamp: str):
    """
    dbt events have a discrepancy in their timestamp formats
    This converts a given timestamp string to %Y-%m-%dT%H:%M:%S.%fZ
    It returns the given timestamp if it couldn't do the conversion
    """
    output_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    input_formats = ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f"]
    for input_format in input_formats:
        try:
            iso_timestamp = datetime.strptime(timestamp, input_format).strftime(output_format)
            return iso_timestamp
        except ValueError:
            pass  # ignore and pass to the other format

    return timestamp


# dbt subcommands, to tell `dbt --debug run` (a flag, then the command) from `dbt --log-path logs run`.
_DBT_COMMANDS = frozenset(
    HANDLED_COMMANDS
    + ["clean", "clone", "compile", "debug", "deps", "docs", "init", "list", "ls", "parse"]
    + ["run-operation", "show", "source"]
)


def get_dbt_command(dbt_command_line: list[str]) -> str | None:
    """The dbt subcommand when dbt-ol handles it, else None.

    The subcommand is the first word that isn't an option or an option's value, so `dbt debug --log-path run`
    is `debug`. A word after an option is taken as its value unless it's a dbt subcommand, because boolean
    flags like `--debug` take no value.
    """
    previous = None
    for token in dbt_command_line[1:]:
        is_option_value = (
            previous is not None
            and previous.startswith("-")
            and "=" not in previous
            and token not in _DBT_COMMANDS
        )
        if not token.startswith("-") and not is_option_value:
            return token if token in HANDLED_COMMANDS else None
        previous = token
    return None


def generate_run_event(
    event_type: RunState,
    event_time: str,
    run_id: str,
    job_name: str,
    job_namespace: str,
    inputs: list[InputDataset] | None = None,
    outputs: list[OutputDataset] | None = None,
    job_facets: dict | None = None,
    run_facets: dict | None = None,
) -> RunEvent:
    inputs = inputs or []
    outputs = outputs or []
    job_facets = job_facets or {}
    run_facets = run_facets or {}
    return RunEvent(
        eventType=event_type,
        eventTime=event_time,
        run=Run(runId=run_id, facets=run_facets),
        job=Job(
            namespace=job_namespace,
            name=job_name,
            facets=job_facets,
        ),
        inputs=inputs,
        outputs=outputs,
        producer=PRODUCER,
    )


def get_dbt_profiles_dir(command: list[str]) -> str:
    """
    Based on https://docs.getdbt.com/docs/core/connect-data-platform/connection-profiles#advanced-customizing-a-profile-directory
    Gets the profiles directory
    """
    from_command = parse_single_arg(command, ["--profiles-dir"])
    from_env_var = os.getenv("DBT_PROFILES_DIR")
    default_dir = os.path.expanduser("~/.dbt/")
    # dbt only falls back to the working directory when it actually holds a
    # profiles.yml, otherwise it uses default_dir.
    cwd = os.getcwd()
    current_working_directory = cwd if os.path.exists(os.path.join(cwd, "profiles.yml")) else None
    return from_command or from_env_var or current_working_directory or default_dir


def get_dbt_log_path(command: list[str]) -> str:
    """
    Based on this https://docs.getdbt.com/reference/global-configs/logs
    Gets the absolute path of the dbt log file.
    If the user doesn't specify the log path, we generate a random name for the logs directory
    """
    project_dir = parse_single_arg(command, ["--project-dir"], default="./") or "./"
    default_log_dirname = os.path.expanduser(os.path.join(project_dir, generate_random_log_file_name()))
    from_command = parse_single_arg(command, ["--log-path"], default=None)
    from_env_var = os.getenv("DBT_LOG_PATH")
    log_dirname: str = from_command or from_env_var or default_log_dirname
    return os.path.join(log_dirname, "dbt.log")


def is_random_logfile(command: list[str]) -> bool:
    from_command = parse_single_arg(command, ["--log-path"], default=None)
    from_env_var = os.getenv("DBT_LOG_PATH")
    if from_env_var or from_command:
        return False
    return True


def generate_random_log_file_name() -> str:
    random_uuid = str(uuid.uuid4())
    log_directory_name = f"dbt-logs-{random_uuid}"
    return log_directory_name


def get_parent_run_metadata_from_context() -> ParentRunMetadata | None:
    """
    Parses the standardized OPENLINEAGE_CONTEXT env var, a JSON payload shaped like:
    {"parent": {"run": {...}, "job": {...}, "root": {"run": {...}, "job": {...}}}}
    Returns None (rather than raising) on any missing/malformed input, so callers can fall back
    to the legacy OPENLINEAGE_PARENT_ID / OPENLINEAGE_ROOT_PARENT_ID env vars.
    """
    raw = os.getenv("OPENLINEAGE_CONTEXT")
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        log.warning(
            "Received OPENLINEAGE_CONTEXT but failed to parse it as JSON; ignoring.",
        )
        log.debug("Exception details", exc_info=True)
        return None

    parent_info = data.get("parent") if isinstance(data, dict) else None
    if not isinstance(parent_info, dict):
        log.debug("OPENLINEAGE_CONTEXT is missing a valid 'parent' key; ignoring")
        return None

    try:
        run, job = parent_info["run"], parent_info["job"]
        run_id, job_namespace, job_name = run["runId"], job["namespace"], job["name"]
    except (KeyError, TypeError) as e:
        log.warning("OPENLINEAGE_CONTEXT.parent is missing required run/job fields; ignoring. Error: %s", e)
        log.debug("Exception details", exc_info=True)
        return None

    root = parent_info.get("root") or {}
    root_run, root_job = root.get("run") or {}, root.get("job") or {}
    # default root to self, matching legacy OPENLINEAGE_ROOT_PARENT_ID-absent behavior
    root_run_id = root_run.get("runId", run_id)
    root_job_namespace = root_job.get("namespace", job_namespace)
    root_job_name = root_job.get("name", job_name)

    return ParentRunMetadata(
        run_id=run_id,
        job_name=job_name,
        job_namespace=job_namespace,
        job_facets=job.get("facets"),
        run_facets=run.get("facets"),
        root_parent_run_id=root_run_id,
        root_parent_job_name=root_job_name,
        root_parent_job_namespace=root_job_namespace,
        root_job_facets=root_job.get("facets"),
        root_run_facets=root_run.get("facets"),
    )


def get_parent_run_metadata():
    """
    The parent job that started the dbt command. Usually the scheduler (Airflow, ...etc)
    """
    from_context = get_parent_run_metadata_from_context()
    if from_context is not None:
        return from_context

    parent_id = os.getenv("OPENLINEAGE_PARENT_ID")
    root_parent_id = os.getenv("OPENLINEAGE_ROOT_PARENT_ID")
    parent_run_metadata = None
    if parent_id:
        parent_tuple = parent_id.split("/")
        if len(parent_tuple) == 3:
            parent_namespace, parent_job_name, parent_run_id = parent_tuple
        else:
            log.warning("Received OPENLINEAGE_PARENT_ID but can't parse it")
            return None

        if root_parent_id:
            root_parent_tuple = root_parent_id.split("/")
            if len(root_parent_tuple) == 3:
                root_parent_job_namespace, root_parent_job_name, root_parent_run_id = root_parent_tuple
            else:
                root_parent_job_namespace, root_parent_job_name, root_parent_run_id = None, None, None
                log.warning("Received OPENLINEAGE_ROOT_PARENT_ID but can't parse it")
        else:
            root_parent_run_id = parent_run_id
            root_parent_job_name = parent_job_name
            root_parent_job_namespace = parent_namespace

        parent_run_metadata = ParentRunMetadata(
            run_id=parent_run_id,
            job_name=parent_job_name,
            job_namespace=parent_namespace,
            root_parent_job_name=root_parent_job_name,
            root_parent_job_namespace=root_parent_job_namespace,
            root_parent_run_id=root_parent_run_id,
        )
    return parent_run_metadata


def get_node_unique_id(event):
    return get_from_nullable_chain(event, ["data", "node_info", "unique_id"])


def get_job_type(event) -> str | None:
    """
    Gets the Run Event's job type
    """
    node_unique_id = get_node_unique_id(event)
    node_type = event["info"]["name"]
    if node_type == "SQLQuery":
        return "SQL"
    elif node_type in ("MainReportVersion", "CommandCompleted"):
        return "JOB"
    elif node_unique_id is None:
        return None
    return job_type_for_unique_id(node_unique_id)


_JOB_TYPES_BY_UNIQUE_ID_PREFIX = (
    ("model.", "MODEL"),
    ("snapshot.", "SNAPSHOT"),
    ("seed.", "SEED"),
    ("test.", "TEST"),
    ("unit_test.", "UNIT_TEST"),
)


def job_type_for_unique_id(unique_id: str) -> str | None:
    """The job type of a dbt node, from its unique_id; None for resources that get no job, like sources."""
    for prefix, job_type in _JOB_TYPES_BY_UNIQUE_ID_PREFIX:
        if unique_id.startswith(prefix):
            return job_type
    return None


# First bytes of native executables: ELF, then Mach-O 32/64-bit in both byte orders and universal binaries.
_NATIVE_EXECUTABLE_MAGICS = (
    b"\x7fELF",
    b"\xce\xfa\xed\xfe",
    b"\xcf\xfa\xed\xfe",
    b"\xfe\xed\xfa\xce",
    b"\xfe\xed\xfa\xcf",
    b"\xca\xfe\xba\xbe",
)
_PIP_SH_LAUNCHER = re.compile(r"exec' \"([^\"]+)\"")
_PYTHON_INTERPRETER = re.compile(r"^(python|pypy)[0-9.]*$")
_DBT_VERSION_LINE = re.compile(r"^dbt (\d+)\.(\d+)\.(\d+)\S*")
# Run with the interpreter of a Python `dbt` script: dbt Core v2 ships the native extension `dbt._core`.
_ENGINE_PROBE = (
    "import importlib.metadata, importlib.util\n"
    "if importlib.util.find_spec('dbt._core'):\n"
    "    print('2', importlib.metadata.version('dbt'))\n"
    "else:\n"
    "    print('1')\n"
)


def is_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in ("1", "true", "yes", "on")


def detect_dbt_engine(executable: str = "dbt") -> tuple[int, str | None]:
    """Which dbt engine the `dbt` on PATH runs: (major version, full version when known).

    dbt Core v1's `dbt --version` looks up the latest releases on PyPI with a 1 s timeout per installed
    package and can't be told not to. So a `dbt` installed next to dbt-ol is read in this interpreter, any
    other one is inspected, and `dbt --version` is only the fallback.
    """
    path = shutil.which(executable)
    if path is None:
        return 1, None
    detected = _engine_in_this_environment(path) or _inspect_dbt_executable(path)
    if detected is not None:
        return detected
    return _dbt_engine_from_version_output(path)


def _engine_in_this_environment(path: str) -> tuple[int, str | None] | None:
    """The engine of a `dbt` installed in dbt-ol's own environment, found without importing dbt.

    Only a console script in this environment's scripts directory runs the `dbt` package this interpreter
    sees; for any other `dbt`, what's importable here says nothing.
    """
    try:
        if not os.path.samefile(os.path.dirname(path), sysconfig.get_path("scripts")):
            return None
        dbt_package = importlib.util.find_spec("dbt")
        if dbt_package is None or not dbt_package.submodule_search_locations:
            return None
        # dbt Core v2 ships the native extension `dbt._core`.
        if importlib.machinery.PathFinder.find_spec(
            "dbt._core", list(dbt_package.submodule_search_locations)
        ):
            return 2, importlib.metadata.version("dbt")
        return 1, None
    except (OSError, ImportError, ValueError):
        return None


def _inspect_dbt_executable(path: str) -> tuple[int, str | None] | None:
    try:
        with open(path, "rb") as executable:
            head = executable.read(512)
    except OSError:
        return None
    # dbt Core v1 is only distributed as a Python package; the standalone installer ships v2 as a binary.
    if head[:4] in _NATIVE_EXECUTABLE_MAGICS:
        return 2, None
    # Windows console scripts are .exe launchers, indistinguishable from a native binary by their header.
    if os.name == "nt" or not head.startswith(b"#!"):
        return None
    interpreter = _console_script_interpreter(head)
    if not interpreter:
        return None
    try:
        probe = subprocess.run(
            [*interpreter, "-c", _ENGINE_PROBE], capture_output=True, text=True, timeout=30, check=False
        )
    except (OSError, subprocess.SubprocessError):
        return None
    words = probe.stdout.split()
    if probe.returncode != 0 or not words:
        return None
    if words[0] == "2":
        return 2, words[1] if len(words) > 1 else None
    return 1, None


def _console_script_interpreter(head: bytes) -> list[str] | None:
    """The command running a Python `dbt` script, or None when the script isn't Python, like a pyenv shim."""
    lines = head.decode("utf-8", errors="replace").splitlines()
    try:
        shebang = shlex.split(lines[0][2:])
    except (IndexError, ValueError):
        return None
    if not shebang:
        return None
    interpreter = shebang[0]
    if os.path.basename(interpreter) == "env":
        # `#!/usr/bin/env [-S] python3`: the first word that isn't an option or a variable assignment.
        interpreter = next((word for word in shebang[1:] if not word.startswith("-") and "=" not in word), "")
    name = os.path.basename(interpreter)
    # pip writes a /bin/sh launcher when the interpreter path is too long for a shebang line.
    if name in ("sh", "bash") and len(lines) > 1:
        match = _PIP_SH_LAUNCHER.search(lines[1])
        return [match.group(1)] if match else None
    if not _PYTHON_INTERPRETER.match(name):
        return None
    return shebang


def _dbt_engine_from_version_output(path: str) -> tuple[int, str | None]:
    try:
        result = subprocess.run([path, "--version"], capture_output=True, text=True, timeout=60, check=False)
    except (OSError, subprocess.SubprocessError):
        log.debug("Running `%s --version` failed; assuming dbt Core v1", path, exc_info=True)
        return 1, None
    for line in result.stdout.splitlines():
        match = _DBT_VERSION_LINE.match(line.strip())
        if match and int(match.group(1)) >= 2:
            return int(match.group(1)), line.strip()[len("dbt ") :]
    return 1, None

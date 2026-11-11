# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
"""Regenerate the dbt Core v2 telemetry fixtures from ./project.

Needs dbt Core v2 on PATH (`pip install dbt`); runs on DuckDB, no warehouse needed:

    python generate_fixtures.py

Writes fixtures/parse_manifest.json (`dbt parse`), fixtures/build.jsonl (the OTel file of `dbt build`) and
fixtures/end_of_run_manifest.json (the manifest the build writes). Manifests keep only the sections the
processor reads; the telemetry drops repetitive DEBUG records; local paths become placeholders.
"""

import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

HERE = Path(__file__).absolute().parent
PROJECT = HERE / "project"
FIXTURES = HERE / "fixtures"
MANIFEST_SECTIONS = ("metadata", "nodes", "sources", "exposures", "unit_tests", "parent_map", "child_map")
KEPT_DEBUG_RECORDS = 10
KEPT_ASSET_PARSED_RECORDS = 4


def run_dbt(args: list[str], env: dict[str, str]) -> None:
    result = subprocess.run(["dbt", *args], cwd=PROJECT, env=env, capture_output=True, text=True)
    # The project fails on purpose (a broken model, a failing test), so only a crash is an error.
    if result.returncode not in (0, 1):
        raise RuntimeError(
            f"dbt {args[0]} exited with {result.returncode}:\n{result.stdout}\n{result.stderr}"
        )


def scrub(text: str, tmp: str) -> str:
    return text.replace(tmp, "<tmp>").replace(str(PROJECT), "<project>")


def write_manifest(source: Path, destination: Path, tmp: str) -> None:
    manifest = json.loads(source.read_text())
    trimmed = {section: manifest[section] for section in MANIFEST_SECTIONS if section in manifest}
    destination.write_text(scrub(json.dumps(trimmed, indent=1, sort_keys=True), tmp) + "\n")


def write_telemetry(source: Path, destination: Path, tmp: str) -> None:
    kept, debug_records, asset_records = [], 0, 0
    for line in source.read_text().splitlines():
        record = json.loads(line)
        name = record.get("event_type", "").rsplit(".", 1)[-1]
        attributes = record.get("attributes") or {}
        if name == "LogMessage" and attributes.get("original_severity_text") == "DEBUG":
            debug_records += 1
            if debug_records > KEPT_DEBUG_RECORDS:
                continue
        if name == "AssetParsed":
            asset_records += 1
            if asset_records > KEPT_ASSET_PARSED_RECORDS:
                continue
        if name == "Invocation" and "raw_command" in attributes:
            # "<venv>/bin/python <venv>/bin/dbt build ..." -> "dbt build ..."
            attributes["raw_command"] = re.sub(r"^.*?\bdbt (?=build\b)", "dbt ", attributes["raw_command"])
        kept.append(scrub(json.dumps(record, sort_keys=True), tmp))
    destination.write_text("\n".join(kept) + "\n")


def main() -> None:
    tmp = tempfile.mkdtemp(prefix="otel-fixtures-")
    try:
        env = {
            **os.environ,
            "DBT_DISABLE_VERSION_CHECK": "1",
            # Otherwise dbt writes an anonymous user id to project/.user.yml.
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            "OTEL_FIXTURE_DUCKDB": os.path.join(tmp, "otel_jaffle.duckdb"),
        }
        common = ["--profiles-dir", ".", "--log-path", os.path.join(tmp, "logs")]
        run_dbt(["parse", *common, "--target-path", os.path.join(tmp, "parse")], env)
        build_env = {**env, "DBT_OTEL_FILE_NAME": os.path.join(tmp, "build.jsonl")}
        run_dbt(["build", *common, "--target-path", os.path.join(tmp, "build")], build_env)

        FIXTURES.mkdir(exist_ok=True)
        write_manifest(Path(tmp, "parse", "manifest.json"), FIXTURES / "parse_manifest.json", tmp)
        write_manifest(Path(tmp, "build", "manifest.json"), FIXTURES / "end_of_run_manifest.json", tmp)
        write_telemetry(Path(tmp, "build.jsonl"), FIXTURES / "build.jsonl", tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()

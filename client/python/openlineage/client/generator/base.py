# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import copy
import json
import logging
import os
import pathlib
import re
import subprocess
import tempfile
from typing import Any

from datamodel_code_generator import DataModelType, PythonVersion
from datamodel_code_generator.imports import Import
from datamodel_code_generator.model import get_data_model_types
from datamodel_code_generator.model import pydantic as pydantic_model
from datamodel_code_generator.parser.jsonschema import JsonSchemaParser
from datamodel_code_generator.types import Types

log = logging.getLogger(__name__)


def camel_to_snake(string: str) -> str:
    """Convert camel_case string to SnakeCase"""
    _under_score_1 = re.compile(r"([^_])([A-Z][a-z]+)")
    _under_score_2 = re.compile("([a-z0-9])([A-Z])")
    subbed = _under_score_1.sub(r"\1_\2", string)
    return _under_score_2.sub(r"\1_\2", subbed).lower()


# import attrs instead of dataclass
DATA_MODELS_TYPES = get_data_model_types(
    DataModelType.DataclassesDataclass, target_python_version=PythonVersion.PY_39
)
NEW_MODEL = DATA_MODELS_TYPES.data_model
NEW_MODEL.DEFAULT_IMPORTS = (Import.from_full_path("attr"),)

# custom code
SET_PRODUCER_CODE = """
PRODUCER = DEFAULT_PRODUCER

def set_producer(producer: str) -> None:
    global PRODUCER  # noqa: PLW0603
    PRODUCER = producer
"""

# locations definitions
FILE_LOCATION = pathlib.Path(__file__).resolve().parent
TEMPLATES_LOCATION = FILE_LOCATION / "templates"

# Global dictionaries intended to be imported by other modules
# Contains schema URLs and base IDs parsed from specification files
SCHEMA_URLS: dict[str, str] = {}
BASE_IDS: dict[str, str] = {}


def deep_merge_dicts(dict1: dict[str, Any], dict2: dict[str, Any]) -> dict[str, Any]:
    """Deep merges two dictionaries.

    This function merges two dictionaries while handling nested dictionaries.
    For keys that exist in both dictionaries, the values from dict2 take precedence.
    If a key exists in both dictionaries and the values are dictionaries themselves,
    they are merged recursively.
    This function merges only dictionaries. If key is of different type, e.g. list
    it does not work properly.
    """
    merged = dict1.copy()
    for k, v in dict2.items():
        if k in merged and isinstance(v, dict):
            merged[k] = deep_merge_dicts(merged.get(k, {}), v)
        else:
            merged[k] = v
    return merged


def parse_additional_data(spec: dict[str, Any], file_name: str) -> None:
    """Parse additional data from spec files.

    Parses:
        * schema URLs
        * base IDs

    Updates the module-level SCHEMA_URLS and BASE_IDS dictionaries
    that can be imported by other modules.
    """
    base_id = spec["$id"]
    for name, _ in spec["$defs"].items():
        SCHEMA_URLS[name] = f"{base_id}#/$defs/{name}"
        BASE_IDS[file_name] = spec["$id"]


def load_specs(base_spec_location: pathlib.Path, facets_spec_location: pathlib.Path) -> list[pathlib.Path]:
    """Load base `OpenLineage.json` and other facets' spec files"""
    locations = []
    if facets_spec_location.is_dir():
        facets_spec_files = sorted(pathlib.Path(os.path.abspath(facets_spec_location)).glob("*.json"))
    else:
        facets_spec_files = [pathlib.Path(os.path.abspath(facets_spec_location))]

    file_specs = [
        base_spec_location.resolve(),  # Base spec must be the first element
        *facets_spec_files,
    ]
    for file_spec in file_specs:
        spec = json.loads(file_spec.read_text())
        parse_additional_data(spec, file_spec.name)
        locations.append(file_spec)

    return locations


def parse_and_generate(
    locations: list[pathlib.Path], extra_template_data: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Parse and generate data models from a given specification."""
    temporary_locations = []

    current_dir = pathlib.Path.cwd()
    with tempfile.TemporaryDirectory() as tmp:
        tmp_directory = pathlib.Path(tmp).resolve()
        for location in locations:
            tmp_location = (tmp_directory / location.name).resolve()
            tmp_location.write_text(location.read_text())
            temporary_locations.append(tmp_location)

        os.chdir(tmp_directory)
        # first parse OpenLineage.json
        parser = JsonSchemaParser(
            source=temporary_locations[:1],
            data_model_type=NEW_MODEL,
            data_model_root_type=DATA_MODELS_TYPES.root_model,
            data_model_field_type=DATA_MODELS_TYPES.field_model,
            data_type_manager_type=DATA_MODELS_TYPES.data_type_manager,
            dump_resolve_reference_action=DATA_MODELS_TYPES.dump_resolve_reference_action,
            special_field_name_prefix="",
            use_schema_description=True,
            field_constraints=True,
            use_union_operator=True,
            use_standard_collections=True,
            base_class="openlineage.client.utils.RedactMixin",
            class_name="ClassToBeSkipped",
            use_field_description=True,
            use_double_quotes=True,
            keep_model_order=True,
            custom_template_dir=TEMPLATES_LOCATION,
            extra_template_data=extra_template_data,  # type: ignore[arg-type]
            additional_imports=[
                "typing.ClassVar",
                "typing.Any",
                "typing.cast",
                "openlineage.client.constants.DEFAULT_PRODUCER",
            ],
        )

        # keep information about uuid and date-time formats
        # this is going to be changed back to str type hint in jinja template
        data_type_manager: pydantic_model.DataTypeManager = parser.data_type_manager  # type: ignore[assignment]
        uuid_type = copy.deepcopy(data_type_manager.type_map[Types.uuid])
        uuid_type.type = "uuid"
        data_type_manager.type_map[Types.uuid] = uuid_type

        date_time_type = copy.deepcopy(data_type_manager.type_map[Types.date_time])
        date_time_type.type = "date-time"
        data_type_manager.type_map[Types.date_time] = date_time_type

        uri_type = copy.deepcopy(data_type_manager.type_map[Types.date_time])
        uri_type.type = "uri"
        data_type_manager.type_map[Types.uri] = uri_type

        parser.parse(format_=False)

        # parse rest of spec
        parser.source = temporary_locations[1:]

        # change paths so that parser sees base objects as local
        parser.model_resolver.references = {
            k.replace("OpenLineage.json", BASE_IDS["OpenLineage.json"]): v
            for k, v in parser.model_resolver.references.items()
        }

        output = parser.parse(format_=False)

    # go back to original directory
    os.chdir(current_dir)

    return output  # type: ignore[return-value]


def separate_imports(code: str) -> tuple[str, str]:
    """Separates a Python script code (as string) into imports and the rest."""
    imports_section = []
    rest_of_code = []
    in_import = False
    for line in code.splitlines():
        if line.startswith(("import ", "from ")):
            in_import = True
        elif in_import and line.strip() == "":
            in_import = False
        if in_import:
            imports_section.append(line)
        else:
            rest_of_code.append(line)
    return ("\n".join(imports_section), "\n".join(rest_of_code))


def format_and_save_output(
    output: str,
    location: pathlib.Path,
    add_set_producer_code: bool | None = False,
    header: str | None = None,
) -> None:
    """Adjust and format generated file."""

    # save temporary file to the same directory that output
    # so that ruff receives same rules
    with tempfile.NamedTemporaryFile(
        "w", prefix=location.stem.lower(), suffix=".py", dir=location.parent, delete=False
    ) as tmpfile:
        if header:
            tmpfile.write(header + "\n")
        output = output.replace("from .OpenLineage", "from openlineage.client.generated.base")
        imports_section, rest_of_code = separate_imports(output)
        tmpfile.write(imports_section)
        if add_set_producer_code:
            tmpfile.write(SET_PRODUCER_CODE)
        tmpfile.write(rest_of_code)
        tmpfile.flush()

    # run ruff lint
    with subprocess.Popen(
        args=["ruff", "check", tmpfile.name, "--fix"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
    ) as lint_process:
        if lint_process.returncode:
            log.warning("Ruff lint failed: %s", lint_process.returncode)

    # run ruff format
    with subprocess.Popen(
        args=["ruff", "format", tmpfile.name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
    ) as format_process:
        if format_process.returncode:
            log.warning("Ruff lint failed: %s", format_process.returncode)

    # move file to output location
    if location.name == "open_lineage.py":
        location = location.with_name("base.py")
    os.rename(tmpfile.name, location)

    if lint_process.returncode or format_process.returncode:
        log.warning("%s failed on ruff.", location)

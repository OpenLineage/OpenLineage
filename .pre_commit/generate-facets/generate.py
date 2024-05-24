# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import copy
import json
import logging
import os
import pathlib
import pkgutil
import re
import shutil
import tempfile
from collections import defaultdict

import click
import jinja2
import yaml
from datamodel_code_generator import DataModelType, PythonVersion
from datamodel_code_generator.imports import Import
from datamodel_code_generator.model import get_data_model_types
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
NEW_MODEL.DEFAULT_IMPORTS = (
    Import.from_full_path("attr.define"),
    Import.from_full_path("attr.field"),
    Import.from_full_path("openlineage.client.utils"),
    Import.from_full_path("typing.ClassVar"),
    Import.from_full_path("typing.List"),
    Import.from_full_path("openlineage.client.constants.DEFAULT_PRODUCER"),
)

# locations definitions
FILE_LOCATION = pathlib.Path(__file__).resolve().parent
PARENT_LOCATION = pathlib.Path(__file__).resolve().parent.parent.parent
BASE_SPEC_LOCATION = PARENT_LOCATION / "spec" / "OpenLineage.json"
FACETS_SPEC_LOCATION = PARENT_LOCATION / "spec" / "facets"
DEFAULT_OUTPUT_LOCATION = PARENT_LOCATION / "client" / "python" / "openlineage" / "client" / "generated"
TEMPLATES_LOCATION = FILE_LOCATION / "templates"
PYTHON_CLIENT_LOCATION = PARENT_LOCATION / "client" / "python"

# custom code
SET_PRODUCER_CODE = """
PRODUCER = DEFAULT_PRODUCER

def set_producer(producer: str) -> None:
    global PRODUCER  # noqa: PLW0603
    PRODUCER = producer
"""

HEADER = (FILE_LOCATION / "header.py").resolve().read_text()

# structures to customize code generation
REDACT_FIELDS = yaml.safe_load((PYTHON_CLIENT_LOCATION / "redact_fields.yml").read_text())
SCHEMA_URLS = {}
BASE_IDS = {}


def get_redact_fields(module_name):
    module_entry = next((entry for entry in REDACT_FIELDS if entry == module_name), None)
    if not module_entry:
        msg = f"{module_name} should be present in redact_fields"
        raise ValueError(msg)
    return {class_name: {"redact_fields": fields} for class_name, fields in module_entry}


def deep_merge_dicts(dict1, dict2):
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


def load_specs(base_spec_location: pathlib.Path, facets_spec_location: pathlib.Path) -> list[pathlib.Path]:
    """Load base `OpenLineage.json` and other facets' spec files"""
    locations = []

    file_specs = [
        base_spec_location.resolve(),  # Base spec must be the first element
        *sorted(pathlib.Path(os.path.abspath(facets_spec_location)).glob("*.json")),
    ]
    for file_spec in file_specs:
        spec = json.loads(file_spec.read_text())
        parse_additional_data(spec, file_spec.name)
        locations.append(file_spec)

    return locations


def parse_additional_data(spec, file_name):
    """Parse additional data from spec files.

    Parses:
        * schema URLs
        * base IDs
    """
    base_id = spec["$id"]
    for name, _ in spec["$defs"].items():
        SCHEMA_URLS[name] = f"{base_id}#/$defs/{name}"
        BASE_IDS[file_name] = spec["$id"]


def parse_and_generate(locations):
    """Parse and generate data models from a given specification."""

    extra_schema_urls = {obj_name: {"_schemaURL": schema_url} for obj_name, schema_url in SCHEMA_URLS.items()}

    extra_redact_fields = defaultdict(lambda: {"redactions": []})

    for module_entry in REDACT_FIELDS:
        for clazz in module_entry["classes"]:
            class_name = clazz["class_name"]
            # defaultdict automatically creates the key-value pair if not found
            extra_redact_fields[class_name]["redactions"].append({
                "fields": clazz["redact_fields"],
                "module_name": module_entry["module"],
            })

    temporary_locations = []
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
            use_union_operator=False,
            use_standard_collections=False,
            base_class="openlineage.client.utils.RedactMixin",
            class_name="ClassToBeSkipped",
            use_field_description=True,
            use_double_quotes=True,
            keep_model_order=False,
            custom_template_dir=TEMPLATES_LOCATION,
            extra_template_data=defaultdict(dict, deep_merge_dicts(extra_redact_fields, extra_schema_urls)),
        )

        # keep information about uuid and date-time formats
        # this is going to be changed back to str type hint in jinja template
        uuid_type = copy.deepcopy(parser.data_type_manager.type_map[Types.uuid])
        uuid_type.type = "uuid"
        parser.data_type_manager.type_map[Types.uuid] = uuid_type

        date_time_type = copy.deepcopy(parser.data_type_manager.type_map[Types.date_time])
        date_time_type.type = "date-time"
        parser.data_type_manager.type_map[Types.date_time] = date_time_type

        uri_type = copy.deepcopy(parser.data_type_manager.type_map[Types.date_time])
        uri_type.type = "uri"
        parser.data_type_manager.type_map[Types.uri] = uri_type

        parser.parse(format_=False)

        # parse rest of spec
        parser.source = temporary_locations[1:]

        # change paths so that parser sees base objects as local
        parser.model_resolver.references = {
            k.replace("OpenLineage.json", BASE_IDS["OpenLineage.json"]): v
            for k, v in parser.model_resolver.references.items()
        }

        output = parser.parse(format_=False)

    # go back to client location
    os.chdir(PYTHON_CLIENT_LOCATION)

    return output


def generate_facet_v2_module(module_location):
    modules = [name for _, name, _ in pkgutil.iter_modules([module_location]) if name != "base"]

    facet_v2_template = (TEMPLATES_LOCATION / "facet_v2.jinja2").read_text()
    output = jinja2.Environment(autoescape=True).from_string(facet_v2_template).render(facets_modules=modules)
    format_and_save_output(
        output=output, location=PYTHON_CLIENT_LOCATION / "openlineage" / "client" / "facet_v2.py"
    )
def separate_imports(code):
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


def format_and_save_output(output: str, location: pathlib.Path, add_set_producer_code: bool = False):
    """Adjust and format generated file."""
    import subprocess

    # save temporary file to the same directory that output
    # so that ruff receives same rules
    with tempfile.NamedTemporaryFile(
        "w", prefix=location.stem.lower(), suffix=".py", dir=DEFAULT_OUTPUT_LOCATION.parent, delete=False
    ) as tmpfile:
        tmpfile.write(HEADER)
        output = output.replace("from .OpenLineage", "from openlineage.client.generated.base")
        imports_section, rest_of_code = separate_imports(output)
        tmpfile.write(imports_section)
        if add_set_producer_code:
            tmpfile.write(SET_PRODUCER_CODE)
        tmpfile.write(rest_of_code)
        tmpfile.flush()

    # run ruff lint
    with subprocess.Popen(
        args=["ruff", "check", tmpfile.name, "--fix"], stderr=subprocess.STDOUT, close_fds=True
    ) as lint_process:
        if lint_process.returncode:
            print(f"Ruff lint failed: {lint_process.returncode}")

    # run ruff format
    with subprocess.Popen(
        args=["ruff", "format", tmpfile.name],
        stderr=subprocess.STDOUT,
        close_fds=True,
    ) as format_process:
        if format_process.returncode:
            print(f"Ruff format failed: {format_process.returncode}")

    # move file to output location
    if location.name == "open_lineage.py":
        location = location.with_name("base.py")
    os.rename(tmpfile.name, location)

    if lint_process.returncode or format_process.returncode:
        log.warning("%s failed on ruff.", location)

@click.command()
@click.option("--output-location", type=pathlib.Path, default=DEFAULT_OUTPUT_LOCATION)
def main(output_location):
    locations = load_specs(base_spec_location=BASE_SPEC_LOCATION, facets_spec_location=FACETS_SPEC_LOCATION)
    results = parse_and_generate(locations)
    modules = {
        output_location.joinpath(*[camel_to_snake(n.replace("Facet", "")) for n in name]): result.body
        for name, result in sorted(results.items())
    }
    shutil.rmtree(output_location)
    for path, body in modules.items():
        if path and not path.parent.exists():
            path.parent.mkdir(parents=True)
        if body:
            format_and_save_output(body, path, path.name == "open_lineage.py")
    generate_facet_v2_module(DEFAULT_OUTPUT_LOCATION)


if __name__ == "__main__":
    main()

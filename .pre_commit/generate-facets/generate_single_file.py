# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import ast
import copy
import json
import os
import pathlib
import sys
import tempfile
from collections import defaultdict

import click
from datamodel_code_generator import DataModelType, PythonVersion
from datamodel_code_generator.imports import Import
from datamodel_code_generator.model import get_data_model_types
from datamodel_code_generator.parser.jsonschema import JsonSchemaParser
from datamodel_code_generator.types import Types

data_model_types = get_data_model_types(
    DataModelType.DataclassesDataclass, target_python_version=PythonVersion.PY_38
)
new_model = data_model_types.data_model
new_model.DEFAULT_IMPORTS = (
    Import.from_full_path("attrs.define"),
    Import.from_full_path("attrs.field"),
)

base_spec_location = pathlib.Path(__file__).resolve().parent.parent.parent / "spec" / "OpenLineage.json"
facets_spec_location = pathlib.Path(__file__).resolve().parent.parent.parent / "spec" / "facets"

default_output_location = (
    pathlib.Path(__file__).resolve().parent.parent.parent
    / "client"
    / "python"
    / "openlineage"
    / "client"
    / "facet.py"
)

set_producer_code = """
PRODUCER = DEFAULT_PRODUCER

def set_producer(producer: str) -> None:
    global PRODUCER  # noqa: PLW0603
    PRODUCER = producer
"""

class_aliases = {
    "Field": "SchemaField",
    "ColumnMetrics": "ColumnMetric",
    "Error": "ExtractionError",
    "EventType": "RunState",
    "Fields": "ColumnLineageDatasetFacetFieldsAdditional",
    "InputField": "ColumnLineageDatasetFacetFieldsAdditionalInputFields",
    "Identifier": "SymlinksDatasetFacetIdentifiers",
    "PreviousIdentifier": "LifecycleStateChangeDatasetFacetPreviousIdentifier",
    "Owner": "OwnershipDatasetFacetOwners",
    "OwnershipJobFacetOwner": "OwnershipJobFacetOwners",
}

redact_fields = {
    "ParentRunFacet": {"redact_fields": ["job", "run"]}
}

validations = {
    "BaseEvent": {"validations": {"eventTime": "date-time"}}
}

schema_urls = {}

header = (
    "# Copyright 2018-2024 contributors to the OpenLineage project\n"
    "# SPDX-License-Identifier: Apache-2.0\n\n"
)


def merge_dicts(dict1, dict2):
    merged = dict1.copy()
    for k, v in dict2.items():
        if k in merged and isinstance(v, dict):
            merged[k] = merge_dicts(merged.get(k, {}), v)
        else:
            merged[k] = v
    return merged


def load_specs(base_spec_location, facets_spec_location):
    """Load base `OpenLineage.json` and other facets' spec files"""
    with open(os.path.abspath(base_spec_location)) as base_file:
        base_spec = json.load(base_file)
        parse_additional_data(base_spec)

    for file_spec in sorted(pathlib.Path(os.path.abspath(facets_spec_location)).glob("*.json")):
        spec = json.load(file_spec.open())
        parse_additional_data(spec)
        base_spec["$defs"] = {**base_spec["$defs"], **spec["$defs"]}

    return base_spec


def parse_additional_data(spec):
    """Parse additional data from spec files.

    Parses:
        * schema URLs
    """
    base_id = spec["$id"]
    for name, _ in spec["$defs"].items():
        schema_urls[name] = f"{base_id}#/$defs/{name}"


class RecursiveVisitor(ast.NodeVisitor):
    """Search for class name in type hints."""

    def __init__(self, name):
        self.node = None
        self.name = name
        self.matched = False

    def visit(self, node):
        if not self.node:
            self.node = node
        super().visit(node)

    def recursive(func):  # noqa: N805
        """decorator to make visitor work recursive"""

        def wrapper(self, node):
            func(self, node)
            for child in ast.iter_child_nodes(node):
                if not self.matched:
                    self.visit(child)

        return wrapper

    @recursive
    def visit_AnnAssign(self, node):  # noqa: N802
        ...

    @recursive
    def visit_Name(self, node):  # noqa: N802
        if node.id == self.name:
            self.matched = True


def remove_digits_on_duplicate_names(body):
    """Remove digits generated from datamodel_code_generator.

    Duplicates should use parent class name instead of incremental digit.
    """
    module = ast.parse(body)
    duplicates = []
    for node in ast.iter_child_nodes(module):
        if isinstance(node, ast.ClassDef) and any(s.isdigit() for s in node.name):
            duplicates.append(node.name)

    for duplicate in duplicates:
        name_without_digits = "".join(s for s in duplicate if not s.isdigit())
        for node in ast.iter_child_nodes(module):
            if isinstance(node, ast.ClassDef):
                visitor = RecursiveVisitor(duplicate)
                visitor.visit(node)
                if visitor.matched:
                    new_name = visitor.node.name + name_without_digits
                    break
        body = body.replace(duplicate, new_name)

    return body


def parse_and_generate(spec):
    """Parse and generate data models from a given specification."""

    extra_schema_urls = {obj_name: {"_schemaURL": schema_url} for obj_name, schema_url in schema_urls.items()}
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=True) as tmpfile:
        json.dump(spec, tmpfile)
        tmpfile.flush()

        parser = JsonSchemaParser(
            source=pathlib.Path(tmpfile.name),
            data_model_type=new_model,
            data_model_root_type=data_model_types.root_model,
            data_model_field_type=data_model_types.field_model,
            data_type_manager_type=data_model_types.data_type_manager,
            dump_resolve_reference_action=data_model_types.dump_resolve_reference_action,
            special_field_name_prefix="",
            use_schema_description=True,
            field_constraints=True,
            use_union_operator=True,
            use_standard_collections=True,
            base_class="openlineage.client.utils.RedactMixin",
            use_field_description=True,
            use_double_quotes=True,
            keep_model_order=True,
            custom_template_dir=pathlib.Path(__file__).resolve().parent / "templates",
            extra_template_data=defaultdict(dict, merge_dicts(redact_fields, extra_schema_urls)),
            additional_imports=["typing.ClassVar", "openlineage.client.constants.DEFAULT_PRODUCER"],
        )

        uuid_type = copy.deepcopy(parser.data_type_manager.type_map[Types.uuid])
        uuid_type.type = 'uuid'
        parser.data_type_manager.type_map[Types.uuid] = uuid_type

        date_time_type = copy.deepcopy(parser.data_type_manager.type_map[Types.date_time])
        date_time_type.type = 'date-time'
        parser.data_type_manager.type_map[Types.date_time] = date_time_type

        return parser.parse(format_=False)


def format_and_save_output(output, location):
    """Adjust and format generated file."""
    import subprocess

    output = remove_digits_on_duplicate_names(output)

    # save temporary file to the same directory that output
    # so that ruff receives same rules
    with tempfile.NamedTemporaryFile(
        "w", suffix=".py", dir=default_output_location.parent, delete=False
    ) as tmpfile:
        tmpfile.write(header)
        tmpfile.write(output)
        tmpfile.write(set_producer_code)
        tmpfile.write("\n# alias class names to keep backwards compatibility")
        for name, alias in class_aliases.items():
            tmpfile.write(f"\n{alias} = {name}")
        tmpfile.flush()

    # run ruff lint
    with subprocess.Popen(
        args=["ruff", tmpfile.name, "--fix"], stderr=subprocess.STDOUT, close_fds=True
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
    os.rename(tmpfile.name, location)

    return lint_process.returncode or format_process.returncode


@click.command()
@click.option("--output-file", type=str, default=default_output_location)
def main(output_file):
    spec = load_specs(base_spec_location=base_spec_location, facets_spec_location=facets_spec_location)
    output = parse_and_generate(spec)
    rc = format_and_save_output(output=output, location=output_file)

    if rc:
        print(rc)
        sys.exit(rc)


if __name__ == "__main__":
    main()

# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pathlib
import pkgutil
import shutil
from collections import defaultdict
from typing import Any

import click
import jinja2  # type: ignore[import-not-found]
import yaml
from base import (  # type: ignore[import-not-found]
    SCHEMA_URLS,
    camel_to_snake,
    deep_merge_dicts,
    format_and_save_output,
    load_specs,
    parse_and_generate,
)

# locations definitions
FILE_LOCATION = pathlib.Path(__file__).resolve().parent
PYTHON_CLIENT_DIR_LOCATION = FILE_LOCATION.parent.parent.parent.parent
PYTHON_CLIENT_SRC_LOCATION = PYTHON_CLIENT_DIR_LOCATION / "src" / "openlineage" / "client"
REPO_ROOT_LOCATION = PYTHON_CLIENT_DIR_LOCATION.parent.parent
BASE_SPEC_LOCATION = REPO_ROOT_LOCATION / "spec" / "OpenLineage.json"
FACETS_SPEC_LOCATION = REPO_ROOT_LOCATION / "spec" / "facets"
DEFAULT_OUTPUT_LOCATION = PYTHON_CLIENT_SRC_LOCATION / "generated"
TEMPLATES_LOCATION = FILE_LOCATION / "templates"

HEADER = (FILE_LOCATION / "header.py").resolve().read_text()

# structures to customize code generation
REDACT_FIELDS = yaml.safe_load((PYTHON_CLIENT_DIR_LOCATION / "redact_fields.yml").read_text())


def get_redact_fields(module_name: str) -> dict[str, dict[str, list[str]]]:
    module_entry = next((entry for entry in REDACT_FIELDS if entry == module_name), None)
    if not module_entry:
        msg = f"{module_name} should be present in redact_fields"
        raise ValueError(msg)
    return {class_name: {"redact_fields": fields} for class_name, fields in module_entry}


def generate_facet_v2_module(module_location: pathlib.Path) -> None:
    modules = [name for _, name, _ in pkgutil.iter_modules([str(module_location)]) if name != "base"]

    facet_v2_template = (TEMPLATES_LOCATION / "facet_v2.jinja2").read_text()
    output = jinja2.Environment(autoescape=True).from_string(facet_v2_template).render(facets_modules=modules)
    format_and_save_output(
        output=output,
        location=PYTHON_CLIENT_SRC_LOCATION / "facet_v2.py",
        header=HEADER,
    )


@click.command()
@click.option("--output-location", type=pathlib.Path, default=DEFAULT_OUTPUT_LOCATION)
def main(output_location: pathlib.Path) -> None:
    locations = load_specs(base_spec_location=BASE_SPEC_LOCATION, facets_spec_location=FACETS_SPEC_LOCATION)

    extra_schema_urls = {obj_name: {"_schemaURL": schema_url} for obj_name, schema_url in SCHEMA_URLS.items()}

    extra_redact_fields: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: {"redactions": []})

    for module_entry in REDACT_FIELDS:
        for clazz in module_entry["classes"]:
            class_name = clazz["class_name"]
            # defaultdict automatically creates the key-value pair if not found
            extra_redact_fields[class_name]["redactions"].append(
                {
                    "fields": clazz["redact_fields"],
                    "module_name": module_entry["module"],
                }
            )

    extra_template_data = defaultdict(dict, deep_merge_dicts(extra_redact_fields, extra_schema_urls))

    results = parse_and_generate(locations, extra_template_data)

    if output_location.exists():
        shutil.rmtree(output_location)
    for name, result in sorted(results.items()):
        path = output_location.joinpath(*[camel_to_snake(n.replace("Facet", "")) for n in name])
        if path and not path.parent.exists():
            path.parent.mkdir(parents=True)
        if result.body:
            format_and_save_output(
                output=result.body,
                location=path,
                add_set_producer_code=path.name == "open_lineage.py",
                header=HEADER,
            )

    # Add empty init file to output directory
    with open(output_location / "__init__.py", "w") as init_file:
        init_file.write(HEADER)
    generate_facet_v2_module(DEFAULT_OUTPUT_LOCATION)


if __name__ == "__main__":
    main()

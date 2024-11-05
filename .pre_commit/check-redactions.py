# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import ast
import pathlib
from collections import namedtuple

import yaml

PARENT_LOCATION = pathlib.Path(__file__).resolve().parent.parent
MODULES_LOCATION = PARENT_LOCATION / "client" / "python" / "openlineage" / "client" / "generated"
REDACT_FIELDS_YAML = PARENT_LOCATION / "client" / "python" / "redact_fields.yml"
REDACT_FIELDS = yaml.safe_load(REDACT_FIELDS_YAML.read_text())
MissingClass = namedtuple("MissingClass", "module_name class_name")


def get_class_names(code):
    """
    Extracts a list of class names from a Python code.
    """
    tree = ast.parse(code)

    # Extract ClassDef nodes
    return [node.name for node in tree.body if isinstance(node, ast.ClassDef)]


if __name__ == "__main__":
    not_found = []
    # find all modules
    for file in MODULES_LOCATION.glob("*.py"):
        module_name = file.stem
        # get all classes defined in module
        classes = get_class_names(file.read_text())
        # check if module is defined in yaml
        redact_module = next((m for m in REDACT_FIELDS if m["module"] == module_name), None)
        if not redact_module:
            not_found.extend([MissingClass(module_name, clazz) for clazz in classes])
            continue
        for clazz in classes:
            # check if class is defined in yaml
            if not next((c for c in redact_module["classes"] if c["class_name"] == clazz), None):
                not_found.append(MissingClass(module_name, clazz))
    if not_found:
        missing_str = "\n\t".join([f"module: {c.module_name}, class: {c.class_name}" for c in not_found])
        msg = (f"Following classes are missing in {REDACT_FIELDS_YAML}. "
               "Even if no redactions are needed, please put the class with empty list:\n\t"
               f"{missing_str}"
        )
        raise SystemExit(msg)

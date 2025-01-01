# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Dict, List, TypeVar

from jinja2 import Undefined
from openlineage.common.provider.dbt.processor import DbtArtifactProcessor
from openlineage.common.utils import get_from_nullable_chain


class SkipUndefined(Undefined):
    def __getattr__(self, name):
        return SkipUndefined(name=f"{self._undefined_name}.{name}")

    def __str__(self):
        return f"{{{{ {self._undefined_name} }}}}"

    def _fail_with_undefined_error(self, *args, **kwargs):
        pass

    def __call__(self, *args, **kwargs):
        arguments = ", ".join(
            [arg._undefined_name if isinstance(arg, SkipUndefined) else str(arg) for arg in args]
        )
        return f"{{{{ {self._undefined_name}({arguments}) }}}}"


T = TypeVar("T")


class DbtCloudArtifactProcessor(DbtArtifactProcessor):
    should_raise_on_unsupported_command = False

    def __init__(self, manifest, run_result, profile, catalog, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.manifest = manifest
        self.run_result = run_result
        self.profile = profile
        self.catalog = catalog

    @classmethod
    def check_metadata_version(
        cls, metadata, desired_schema_versions: List[int], logger: logging.Logger
    ) -> None:
        str_schema_version = get_from_nullable_chain(metadata, ["metadata", "dbt_schema_version"])
        schema_version = cls.get_schema_version(metadata)
        if schema_version not in desired_schema_versions:
            if schema_version > max(desired_schema_versions):
                logger.warning(
                    f"Artifact schema version: {str_schema_version} is above dbt-ol "
                    f"supported version {max(desired_schema_versions)}. "
                    f"This might cause errors."
                )
            else:
                raise ValueError(
                    f"Wrong version of dbt metadata: {schema_version}, "
                    f"should be in {desired_schema_versions}"
                )

    def get_dbt_metadata(self):
        self.check_metadata_version(self.manifest, list(range(2, 13)), self.logger)
        self.check_metadata_version(self.run_result, [2, 3, 4, 5, 6], self.logger)

        return self.manifest, self.run_result, self.profile, self.catalog

    def extract_namespace(self, profile: Dict) -> str:
        return super().extract_namespace(profile["details"])

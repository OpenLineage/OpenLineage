# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple, TypeVar

import yaml
from jinja2 import Environment, Undefined
from openlineage.common.provider.dbt.processor import DbtArtifactProcessor
from openlineage.common.utils import get_from_nullable_chain

DBT_TARGET_PATH_ENVVAR = "DBT_TARGET_PATH"
DEFAULT_TARGET_PATH = "target"


class SkipUndefined(Undefined):
    def __getattr__(self, name):
        return SkipUndefined(name=f"{self._undefined_name}.{name}")

    def __str__(self):
        return f"{{{{ {self._undefined_name} }}}}"

    def _fail_with_undefined_error(self, *args, **kwargs):
        pass

    def __call__(self, *args, **kwargs):
        arguments = ", ".join(
            [
                arg._undefined_name if isinstance(arg, SkipUndefined) else str(arg)
                for arg in args
            ]
        )
        return f"{{{{ {self._undefined_name}({arguments}) }}}}"


T = TypeVar("T")


class DbtLocalArtifactProcessor(DbtArtifactProcessor):
    should_raise_on_unsupported_command = True

    def __init__(
        self,
        project_dir: str,
        profile_name: Optional[str] = None,
        target: Optional[str] = None,
        target_path: Optional[str] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.jinja_environment: Optional[Environment] = None

        absolute_dir = os.path.abspath(project_dir)
        dbt_project = self.load_yaml_with_jinja(
            os.path.join(project_dir, "dbt_project.yml")
        )
        self.target_path = target_path
        target_path = self.build_target_path(dbt_project)

        self.manifest_path = os.path.join(
            absolute_dir, target_path, "manifest.json"
        )
        self.run_result_path = os.path.join(
            absolute_dir, target_path, "run_results.json"
        )
        self.catalog_path = os.path.join(
            absolute_dir, target_path, "catalog.json"
        )

        self.target = target
        self.project_name = dbt_project["name"]
        self.profile_name = profile_name or dbt_project.get("profile")
        if not self.profile_name:
            raise KeyError(f"profile not found in {dbt_project}")

    def build_target_path(self, dbt_project: dict, target_path: Optional[str] = None) -> str:
        """
        Build dbt target path. Uses the following:
        1. target_path (user-defined value, normally given in --target-path CLI flag)
        2. DBT_TARGET_PATH environment variable
        3. target-path in dbt_project.yml
        4. default ("target")

        Precedence order: user-defined target_path > env var > dbt_project.yml > default

        Reference:
        https://docs.getdbt.com/reference/project-configs/target-path
        """
        return self.target_path or \
            os.getenv(DBT_TARGET_PATH_ENVVAR) or \
            dbt_project.get("target-path") or \
            DEFAULT_TARGET_PATH


    @classmethod
    def load_metadata(
        cls, path: str, desired_schema_versions: List[int], logger: logging.Logger
    ) -> Dict[Any, Any]:
        with open(path, "r") as f:
            metadata = json.load(f)
            str_schema_version = get_from_nullable_chain(
                metadata, ["metadata", "dbt_schema_version"]
            )
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
            return metadata

    @staticmethod
    def env_var(var: str, default: Optional[str] = None) -> str:
        """The env_var() function. Return the environment variable named 'var'.
        If there is no such environment variable set, return the default.

        If the default is None, raise an exception for an undefined variable.
        """
        if var in os.environ:
            return os.environ[var]
        elif default is not None:
            return default
        else:
            msg = f"Env var required but not provided: '{var}'"
            raise Exception(msg)

    @staticmethod
    def load_yaml(path: str) -> Dict:
        with open(path, "r") as f:
            return yaml.safe_load(f)

    @staticmethod
    def setup_jinja() -> Environment:
        env = Environment(extensions=["jinja2.ext.do"], undefined=SkipUndefined)
        # When using env vars for Redshift port, it must be "{{ env_var('PORT') | as_number }}"
        # otherwise Redshift driver will complain, hence the need to add the "as_number" filter
        env.filters.update({"as_number": lambda x: x})
        env.globals["env_var"] = DbtLocalArtifactProcessor.env_var
        return env

    def load_yaml_with_jinja(
        self, path: str, include_section: Optional[List[Optional[str]]] = None
    ) -> Dict:
        loaded = self.load_yaml(path)
        if not self.jinja_environment:
            self.jinja_environment = self.setup_jinja()
        return self.render_values_jinja(
            environment=self.jinja_environment,
            value=loaded,
            include_section=include_section,
        )

    @classmethod
    def render_values_jinja(
        cls,
        environment: Environment,
        value: T,
        include_section: Optional[List[Optional[str]]] = None,
    ) -> T:
        """
        Traverses passed dictionary and render any string value using jinja.
        Returns copy of the dict with parsed values.
        """
        include_section = include_section or []
        if isinstance(value, dict):
            parsed_dict = {}
            for key, val in value.items():
                if include_section and key != include_section[0]:
                    continue
                parsed_dict[key] = cls.render_values_jinja(
                    environment, val, include_section=include_section[1:]
                )
            return parsed_dict  # type: ignore
        elif isinstance(value, list):
            parsed_list = []
            for elem in value:
                parsed_list.append(cls.render_values_jinja(environment, elem))
            return parsed_list  # type: ignore
        elif isinstance(value, str):
            return environment.from_string(value).render()  # type: ignore
        else:
            return value

    def get_dbt_metadata(self) -> Tuple[
        Dict[Any, Any], Dict[Any, Any], Dict[Any, Any], Optional[Dict[Any, Any]]
    ]:
        manifest = self.load_metadata(
            self.manifest_path, [2, 3, 4, 5, 6, 7], self.logger
        )

        run_result = self.load_metadata(self.run_result_path, [2, 3, 4, 5], self.logger)

        try:
            catalog: Optional[Dict[Any, Any]] = self.load_metadata(
                self.catalog_path, [1], self.logger
            )
        except FileNotFoundError:
            catalog = None

        profile_dir = run_result["args"]["profiles_dir"]

        profile = self.load_yaml_with_jinja(
            os.path.join(profile_dir, "profiles.yml"),
            include_section=[self.profile_name],
        )[self.profile_name]

        if not self.target:
            self.target = profile["target"]

        profile = profile["outputs"][self.target]

        return manifest, run_result, profile, catalog

# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
"""State and helpers shared by the processors that turn a running dbt process into OpenLineage events.

DbtStructuredLogsProcessor reads dbt Core v1 structured logs, DbtOtelProcessor reads dbt Core v2 OpenTelemetry
output. Both resolve nodes, inputs and test assertions from the manifest in the same way.
"""

import os
from collections import defaultdict
from functools import cached_property, wraps
from typing import NamedTuple

from openlineage.client.event_v2 import Dataset
from openlineage.client.facet_v2 import (
    data_quality_assertions_dataset as dq,
)
from openlineage.client.facet_v2 import (
    extraction_error_run,
    processing_engine_run,
)
from openlineage.client.uuid import generate_new_uuid
from openlineage.common.provider.dbt.facets import (
    DbtRunRunFacet,
    DbtVersionRunFacet,
    ParentRunMetadata,
)
from openlineage.common.provider.dbt.local import DbtLocalArtifactProcessor
from openlineage.common.provider.dbt.processor import ModelNode, expected_from_test_config
from openlineage.common.provider.dbt.utils import (
    __version__ as openlineage_version,
)
from openlineage.common.provider.dbt.utils import (
    get_dbt_command,
    get_parent_run_metadata,
)
from openlineage.common.utils import get_from_nullable_chain


class ManifestIntegrityResult(NamedTuple):
    """Result of manifest integrity validation.

    Attributes:
        is_valid: bool indicating if manifest is valid
        missing_nodes: list of node IDs that are missing
        missing_parents: list of parent node IDs that are missing
        missing_children: list of child node IDs that are missing
        total_missing: total count of missing nodes
        parent_map_size: total number of entries in parent_map
        available_nodes: total number of available nodes (nodes + sources)
    """

    is_valid: bool
    missing_nodes: list[str]
    missing_parents: list[str]
    missing_children: list[str]
    total_missing: int
    parent_map_size: int
    available_nodes: int


def handle_keyerror(func):
    """
    Handle KeyError exceptions when node_id was not found in the dbt manifest.
    """

    @wraps(func)
    def wrapper(self, node_id: str, *args, **kwargs):
        try:
            return func(self, node_id, *args, **kwargs)
        except KeyError:
            self.logger.warning(
                "Did not find node_id %s in the dbt manifest - did "
                "the manifest change while the job was executing?",
                node_id,
            )
            self._extraction_errors[node_id].append(
                extraction_error_run.Error(
                    errorMessage=f"Failed to extract metadata in {func.__name__}: "
                    f"node_id '{node_id}' not found in manifest",
                    task=func.__name__,
                )
            )
            return None

    return wrapper


class DbtLiveProcessor(DbtLocalArtifactProcessor):
    """Base for processors that run dbt and emit events while it runs.

    Subclasses set ``profiles_dir`` and load ``_compiled_manifest``.
    """

    should_raise_on_unsupported_command = True

    def __init__(
        self,
        dbt_command_line: list[str],
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.dbt_command_line: list[str] = dbt_command_line
        # No run_results.json on the live paths; read the flag off the command line instead.
        self.full_refresh = self._full_refresh_from_command_line(self.dbt_command_line)
        self.profiles_dir: str = ""
        self.parent_run_metadata: ParentRunMetadata | None = get_parent_run_metadata()

        self.node_id_to_ol_run_id: dict[str, str] = defaultdict(lambda: str(generate_new_uuid()))
        self.node_id_to_inputs: dict[str, list[ModelNode]] = {}
        self._extraction_errors: dict[str, list[extraction_error_run.Error]] = defaultdict(list)

        # populated while dbt runs
        self._compiled_manifest: dict = {}
        self._dbt_version: str | None = None
        self._dbt_invocation_id: str | None = None

        self.dbt_command_return_code = 0

    @property
    def compiled_manifest(self) -> dict:
        """The manifest nodes, inputs and tests are resolved against; empty until one is loaded."""
        return self._compiled_manifest

    @property
    def catalog(self) -> dict | None:
        return None

    def _node_metadata(self, node_id: str) -> dict:
        """The manifest entry of a node or unit test, or an empty dict."""
        try:
            return self._manifest_entry(node_id)
        except KeyError:
            return {}

    def _manifest_entry(self, node_id: str) -> dict:
        """The manifest entry of a node, source or unit test; raises KeyError when there is none.

        Looks each section up directly: merging the sections per call is quadratic on large projects.
        """
        for section in ("nodes", "sources", "unit_tests"):
            entry = self.compiled_manifest.get(section, {}).get(node_id)
            if entry is not None:
                return entry
        raise KeyError(node_id)

    @staticmethod
    def _full_refresh_from_command_line(command_line: list[str]) -> bool | None:
        """Detect the --full-refresh flag on the dbt command line.

        dbt resolves --full-refresh / --no-full-refresh last-occurrence-wins, so the last
        matching token decides. Returns ``None`` when neither is present so the facet omits
        the flag rather than assuming a default.
        """
        full_refresh: bool | None = None
        for token in command_line:
            if token in ("--full-refresh", "-f"):
                full_refresh = True
            elif token == "--no-full-refresh":
                full_refresh = False
        return full_refresh

    @cached_property
    def dbt_command(self) -> str | None:
        return get_dbt_command(self.dbt_command_line)

    @property
    def dbt_version(self) -> str | None:
        """
        Extracted from the first structured log MainReportVersion
        """
        return self._dbt_version

    @property
    def invocation_id(self) -> str | None:
        return self._dbt_invocation_id

    @cached_property
    def profile(self):
        profile_dict = self.load_yaml_with_jinja(
            os.path.expanduser(os.path.join(self.profiles_dir, "profiles.yml"))
        )[self.profile_name]
        if not self.target:
            self.target = profile_dict["target"]

        current_profile = profile_dict["outputs"][self.target]
        return current_profile

    def _exposures_manifest(self) -> dict:
        return self.compiled_manifest

    @handle_keyerror
    def _get_attached_datasets(self, test_node_id: str) -> list[Dataset]:
        """
        Gets the input datasets the data test is attached to.

        For generic tests, returns the single dataset referenced by attached_node.
        For singular tests (no attached_node), returns all parent datasets from
        parent_map — these are models/sources referenced via ref() and source()
        in the singular test's SQL.
        """
        test_node = self._manifest_entry(test_node_id)
        attached_node_id = test_node.get("attached_node")
        if attached_node_id:
            attached_model_node = self._get_model_node(attached_node_id)
            if attached_model_node:
                return [self.node_to_dataset(node=attached_model_node, has_facets=True)]
            return []
        # Singular test: use parent_map to find all referenced models/sources
        return [
            self.node_to_dataset(node=model_node, has_facets=True)
            for model_node in self._get_model_inputs(test_node_id)
        ]

    @handle_keyerror
    def _get_assertion(
        self, node_id: str, success: bool, num_failures: int | None = None
    ) -> dq.Assertion | None:
        manifest_test_node = self.compiled_manifest["nodes"][node_id]
        test_metadata = manifest_test_node.get("test_metadata")
        if test_metadata:
            name = test_metadata["name"]
            column = get_from_nullable_chain(test_metadata, ["kwargs", "column_name"])
        else:
            # Singular test — no test_metadata, use node name directly
            name = manifest_test_node["name"]
            column = None

        config = manifest_test_node.get("config", {})

        # Extract severity, normalize to lowercase
        severity = config.get("severity")
        if severity:
            severity = severity.lower()

        actual = str(num_failures) if num_failures is not None else None
        expected = expected_from_test_config(config, severity) if num_failures is not None else None

        return dq.Assertion(
            assertion=name,
            name=manifest_test_node.get("name"),
            success=success,
            column=column,
            severity=severity,
            actual=actual,
            expected=expected,
        )

    def _setup_dbt_run_metadata(self, start_event):
        """
        The dbt command defines the parent run metadata of
        all subsequent OL events (NodeStart, NodeFinish ...)
        """
        root_parent_run_id = start_event.run.runId
        root_parent_job_name = start_event.job.name
        root_parent_job_namespace = start_event.job.namespace
        if start_event.run.facets.get("parent"):
            parent_facet = start_event.run.facets.get("parent")
            if hasattr(parent_facet, "root"):
                root_parent_run_id = parent_facet.root.run.runId
                root_parent_job_name = parent_facet.root.job.name
                root_parent_job_namespace = parent_facet.root.job.namespace
            else:
                root_parent_run_id = parent_facet.run.runId
                root_parent_job_name = parent_facet.job.name
                root_parent_job_namespace = parent_facet.job.namespace
        self.dbt_run_metadata = ParentRunMetadata(
            run_id=start_event.run.runId,
            job_name=start_event.job.name,
            job_namespace=start_event.job.namespace,
            root_parent_run_id=root_parent_run_id,
            root_parent_job_name=root_parent_job_name,
            root_parent_job_namespace=root_parent_job_namespace,
        )

    # TODO: remove after deprecation period
    def dbt_version_facet(self) -> dict[str, DbtVersionRunFacet]:
        if not self.dbt_version:
            return {}
        self.logger.debug(
            "dbt_version facet is deprecated, and will be removed in future versions. "
            "Use processing_engine facet instead."
        )
        return {"dbt_version": DbtVersionRunFacet(version=self.dbt_version)}

    def dbt_run_run_facet(self) -> dict[str, DbtRunRunFacet]:
        if not self.invocation_id:
            return {}
        return {
            "dbt_run": DbtRunRunFacet(
                invocation_id=self.invocation_id,
                project_name=self.project_name,
                project_version=self.project_version,
                profile_name=self.profile_name,
                dbt_runtime="core",
                full_refresh=self.full_refresh,
            )
        }

    def processing_engine_facet(self) -> dict[str, processing_engine_run.ProcessingEngineRunFacet]:
        if not self.dbt_version:
            return {}
        return {
            "processing_engine": processing_engine_run.ProcessingEngineRunFacet(
                name="dbt",
                version=self.dbt_version,
                openlineageAdapterVersion=openlineage_version,
            )
        }

    def _get_extraction_error_facet(
        self, node_id: str
    ) -> extraction_error_run.ExtractionErrorRunFacet | None:
        errors = self._extraction_errors.pop(node_id, [])
        if not errors:
            return None
        return extraction_error_run.ExtractionErrorRunFacet(
            totalTasks=len(errors),
            failedTasks=len(errors),
            errors=errors,
        )

    @handle_keyerror
    def _get_model_node(self, node_id) -> ModelNode | None:
        """
        Builds a ModelNode of a given node_id
        """
        if node_id in self.compiled_manifest["nodes"]:
            node_type = "model"
            manifest_node = self.compiled_manifest["nodes"][node_id]
        elif node_id in self.compiled_manifest["sources"]:
            node_type = "source"
            manifest_node = self.compiled_manifest["sources"][node_id]
        else:
            raise KeyError(node_id)
        catalog_node = get_from_nullable_chain(self.catalog, ["nodes", node_id])
        return ModelNode(type=node_type, metadata_node=manifest_node, catalog_node=catalog_node)

    @handle_keyerror
    def _get_node_tags(self, node_id: str) -> list[str]:
        """
        Extract tags from a dbt node in the compiled manifest
        """
        manifest_node = self._manifest_entry(node_id)
        return manifest_node.get("tags", [])

    @handle_keyerror
    def _get_node_meta(self, node_id: str) -> dict:
        """
        Extract the free-form ``meta`` map from a dbt node in the compiled manifest
        """
        manifest_node = self._manifest_entry(node_id)
        return manifest_node.get("meta", {})

    def _get_model_inputs(self, node_id) -> list[ModelNode]:
        """
        Builds the model's upstream inputs
        """
        if node_id in self.node_id_to_inputs:
            return self.node_id_to_inputs[node_id]

        try:
            input_node_ids = self.compiled_manifest["parent_map"][node_id]
            inputs = [node for n in input_node_ids if (node := self._get_model_node(n)) is not None]
            self.node_id_to_inputs[node_id] = inputs
            return inputs
        except KeyError:
            self.logger.warning(
                "Did not find node_id %s in the dbt manifest - did "
                "the manifest change while the job was executing?",
                node_id,
            )
        return []

    def _validate_manifest_integrity(self) -> ManifestIntegrityResult:
        """
        Performs metadata integrity checks on the compiled manifest.

        Validates that all nodes referenced in parent_map exist in the manifest's
        node definitions (either in 'nodes' or 'sources' sections).

        Returns:
            ManifestIntegrityResult: Validation results with integrity check information.
        """
        if not self._compiled_manifest:
            return ManifestIntegrityResult(
                is_valid=True,
                missing_nodes=[],
                missing_parents=[],
                missing_children=[],
                total_missing=0,
                parent_map_size=0,
                available_nodes=0,
            )

        parent_map = self._compiled_manifest.get("parent_map", {})
        all_nodes = {**self._compiled_manifest.get("nodes", {}), **self._compiled_manifest.get("sources", {})}

        missing_nodes = []
        missing_parents = []
        missing_children = []

        for parent_node_id, child_node_ids in parent_map.items():
            # Check that the parent node exists
            if parent_node_id not in all_nodes:
                missing_nodes.append(parent_node_id)
                missing_parents.append(parent_node_id)

            # Check that all child nodes exist
            for child_node_id in child_node_ids:
                if child_node_id not in all_nodes:
                    missing_nodes.append(child_node_id)
                    missing_children.append(child_node_id)

        # Remove duplicates while preserving order
        missing_nodes = list(dict.fromkeys(missing_nodes))
        missing_parents = list(dict.fromkeys(missing_parents))
        missing_children = list(dict.fromkeys(missing_children))

        if missing_nodes:
            self.logger.warning(
                "Manifest integrity check failed: Found %d nodes in parent_map that don't exist "
                "in manifest node definitions, printing first %d of them: %s",
                len(missing_nodes),
                min(10, len(missing_nodes)),
                missing_nodes[: min(10, len(missing_nodes))],  # Log first 10 to avoid overwhelming logs
            )

        return ManifestIntegrityResult(
            is_valid=len(missing_nodes) == 0,
            missing_nodes=missing_nodes,
            missing_parents=missing_parents,
            missing_children=missing_children,
            total_missing=len(missing_nodes),
            parent_map_size=len(parent_map),
            available_nodes=len(all_nodes),
        )

    def get_dbt_metadata(
        self,
    ):
        """
        Replaced by properties
        """
        pass

    def get_root_parent_run_id(self):
        if self.parent_run_metadata is not None:
            return self.parent_run_metadata.root_parent_run_id
        if self.dbt_run_metadata is not None:
            return self.dbt_run_metadata.root_parent_run_id
        return None

    def get_root_parent_job_namespace(self):
        if self.parent_run_metadata is not None:
            return self.parent_run_metadata.root_parent_job_namespace
        if self.dbt_run_metadata is not None:
            return self.dbt_run_metadata.root_parent_job_namespace
        return None

    def get_root_parent_job_name(self):
        if self.parent_run_metadata is not None:
            return self.parent_run_metadata.root_parent_job_name
        if self.dbt_run_metadata is not None:
            return self.dbt_run_metadata.root_parent_job_name
        return None

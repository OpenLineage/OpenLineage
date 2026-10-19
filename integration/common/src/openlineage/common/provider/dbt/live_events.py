# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
"""State and helpers shared by the processors that turn a running dbt process into OpenLineage events.

DbtStructuredLogsProcessor reads dbt Core v1 structured logs, DbtOtelProcessor reads dbt Core v2 OpenTelemetry
output. Both resolve nodes, inputs and test assertions from the manifest in the same way.
"""

import os
from collections import defaultdict
from functools import cached_property, wraps

from openlineage.client.event_v2 import Dataset, InputDataset, OutputDataset, RunState
from openlineage.client.facet_v2 import (
    data_quality_assertions_dataset as dq,
)
from openlineage.client.facet_v2 import (
    extraction_error_run,
    processing_engine_run,
    test_run,
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

    def _test_datasets_with_assertion(
        self, test_node_id: str, success: bool, num_failures: int | None
    ) -> list[InputDataset] | None:
        """The datasets a data test checks, each with the test's assertion; None when it has no assertion."""
        assertion = self._get_assertion(test_node_id, success, num_failures)
        if assertion is None:
            return None
        assertion_facet = dq.DataQualityAssertionsDatasetFacet(assertions=[assertion])
        return [
            Dataset(  # type: ignore[misc]
                name=attached.name,
                namespace=attached.namespace,
                facets={**attached.facets, "dataQualityAssertions": assertion_facet},  # type: ignore[dict-item]
            )
            for attached in self._get_attached_datasets(test_node_id) or []
        ]

    def _singular_test_facet(
        self, test_node_id: str, success: bool, num_failures: int | None
    ) -> test_run.TestRunFacet | None:
        """The test facet of a singular test (a plain SQL file).

        Generic tests (not_null, unique, etc.) have test_metadata and are covered by the dataQualityAssertions
        facet on the dataset they check.
        """
        test_node = self._node_metadata(test_node_id)
        if test_node.get("test_metadata") is not None:
            return None
        config = test_node.get("config") or {}
        severity = (config.get("severity") or "error").lower()
        test_execution = test_run.TestExecution(
            name=test_node_id, status="pass" if success else "fail", severity=severity
        )
        test_execution.type = "singular"
        test_execution.content = test_node.get("compiled_code") or test_node.get("raw_code")
        test_execution.contentType = "sql"
        if description := test_node.get("description"):
            test_execution.description = description
        if num_failures is not None:
            test_execution.actual = str(num_failures)
            test_execution.expected = expected_from_test_config(config, severity)
        return test_run.TestRunFacet(tests=[test_execution])

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

    def _node_inputs(self, node_id: str) -> list[ModelNode]:
        """The upstream inputs a node's events report."""
        return self._get_model_inputs(node_id)

    def _node_end_datasets(
        self,
        node_id: str,
        output_node: ModelNode | None,
        event_type: RunState,
        compiled_sql: str | None = None,
    ) -> tuple[list[InputDataset], list[OutputDataset]]:
        """The inputs and output of a node's final event.

        Exposures are attached only when the node succeeded. A model or snapshot that ran gets column
        lineage from ``compiled_sql``, or else from the compiled code in its output node's manifest entry.
        """
        succeeded = event_type == RunState.COMPLETE
        inputs = []
        for model_input in self._node_inputs(node_id):
            input_dataset = self.node_to_dataset(node=model_input, has_facets=True)
            if succeeded:
                input_dataset.facets.update(  # type: ignore[union-attr]
                    self._exposures_facets(model_input.metadata_node.get("unique_id"))
                )
            inputs.append(input_dataset)
        if output_node is None:
            return inputs, []  # type: ignore[return-value]
        output_dataset = self.node_to_output_dataset(node=output_node, has_facets=True)
        if node_id.startswith(("model.", "snapshot.")):
            if event_type in (RunState.COMPLETE, RunState.FAIL):
                compiled_sql = (
                    compiled_sql
                    or output_node.metadata_node.get("compiled_code")
                    or output_node.metadata_node.get("compiled_sql")
                )
                if compiled_sql and (
                    column_lineage := self.get_column_lineage(output_dataset.namespace, compiled_sql)
                ):
                    output_dataset.facets["columnLineage"] = column_lineage  # type: ignore[index]
            if succeeded:
                output_dataset.facets.update(self._exposures_facets(node_id))  # type: ignore[union-attr]
        return inputs, [output_dataset]  # type: ignore[return-value]

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

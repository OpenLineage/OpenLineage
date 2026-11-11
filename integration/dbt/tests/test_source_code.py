# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import subprocess
from unittest import mock

import pytest
from openlineage.client.client import OpenLineageClient
from openlineage.client.transport.noop import NoopConfig, NoopTransport


def git(directory, *args):
    return subprocess.run(["git", "-C", str(directory), *args], check=True, capture_output=True, text=True)


@pytest.fixture
def repository(tmp_path):
    """Create an isolated repository containing a nested dbt project."""
    repo = tmp_path / "repository"
    repo.mkdir()
    git(repo, "init")
    git(repo, "remote", "add", "origin", "https://github.com/example/analytics.git")
    git(
        repo,
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.com",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "--allow-empty",
        "-m",
        "Initial commit",
    )
    return repo


def test_live_events_include_project_path_and_preserve_existing_facet(repository, monkeypatch):
    """Live events receive the project prefix without replacing a producer-supplied source facet."""
    from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
    from openlineage.client.facet_v2 import source_code_location_job
    from openlineage.dbt import emit_live_events

    project = repository / "selected"
    project.mkdir()
    existing = source_code_location_job.SourceCodeLocationJobFacet(
        type="git", url="https://example.com/other", path="existing"
    )
    events = [
        RunEvent(
            eventType=RunState.COMPLETE,
            eventTime="2026-11-04T00:00:00Z",
            run=Run(runId="987b54dc-7537-5edc-808e-cf56dbc77e7c"),
            job=Job(namespace="test", name=name, facets=facets),
        )
        for name, facets in [("dbt-run", {}), ("model", {}), ("existing", {"sourceCodeLocation": existing})]
    ]
    client = OpenLineageClient(
        source_code_location_dir=str(project),
        config={"facets": {"source_code_location": {"disabled": False}}},
        transport=NoopTransport(NoopConfig()),
    )
    client.emit = mock.Mock(wraps=client.emit)

    def create_client(**kwargs):
        assert kwargs == {"source_code_location_dir": str(project)}
        return client

    monkeypatch.setattr("openlineage.dbt.OpenLineageClient", create_client)

    def parse():
        yield from events

    processor = mock.MagicMock(parse=parse, dbt_command_return_code=0)
    assert emit_live_events(processor, mock.MagicMock(), str(project)) == 0
    assert client.emit.call_count == 3
    assert events[0].job.facets["sourceCodeLocation"].path == "selected"
    assert events[1].job.facets["sourceCodeLocation"].path == "selected"
    assert events[2].job.facets["sourceCodeLocation"] is existing


def test_artifact_wrapper_events_include_project_path(repository, monkeypatch):
    """Artifact replay emits the repository-relative directory of the selected project."""
    from openlineage.dbt import consume_local_artifacts

    project = repository / "analytics"
    project.mkdir()
    results = project / "run_results.json"
    results.write_text("{}")
    processor = mock.MagicMock(job_name="dbt-run-analytics", run_result_path=str(results))
    processor.parse.return_value.events.return_value = []
    client = OpenLineageClient(
        source_code_location_dir=str(project),
        config={"facets": {"source_code_location": {"disabled": False}}},
        transport=NoopTransport(NoopConfig()),
    )
    client.emit = mock.Mock(wraps=client.emit)

    def create_client(**kwargs):
        assert kwargs == {"source_code_location_dir": str(project)}
        return client

    monkeypatch.setattr("openlineage.dbt.OpenLineageClient", create_client)
    monkeypatch.setattr("openlineage.dbt.DbtLocalArtifactProcessor", lambda **kwargs: processor)
    monkeypatch.setattr("openlineage.dbt.get_parent_run_metadata", lambda: None)

    assert (
        consume_local_artifacts(
            args=["send-events"],
            target=None,
            target_path=None,
            project_dir=str(project),
            profile_name=None,
            model_selector=None,
            models=[],
        )
        == 0
    )
    assert client.emit.call_count == 2
    for call in client.emit.call_args_list:
        assert call.args[0].job.facets["sourceCodeLocation"].path == "analytics"

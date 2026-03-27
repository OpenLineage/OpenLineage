# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import subprocess
from unittest.mock import MagicMock, patch

import pytest
from openlineage.client.client import OpenLineageClient, OpenLineageConfig
from openlineage.client.facets import FacetsConfig, SourceCodeLocationConfig
from openlineage.client.run import Job, JobEvent, Run, RunEvent, RunState
from openlineage.client.transport.noop import NoopConfig, NoopTransport
from openlineage.client.utils import _find_git_root, _get_git_snapshot, get_git_repo_url
from openlineage.client.uuid import generate_new_uuid


@pytest.fixture(autouse=True)
def _no_git_autodetect():
    """Override the conftest fixture so SCL tests control git detection themselves."""
    yield


# ---------------------------------------------------------------------------
# _find_git_root
# ---------------------------------------------------------------------------
class TestFindGitRoot:
    def test_finds_root_in_current_dir(self, tmp_path):
        (tmp_path / ".git").mkdir()
        assert _find_git_root(str(tmp_path)) == str(tmp_path)

    def test_finds_root_in_parent(self, tmp_path):
        (tmp_path / ".git").mkdir()
        subdir = tmp_path / "a" / "b" / "c"
        subdir.mkdir(parents=True)
        assert _find_git_root(str(subdir)) == str(tmp_path)

    def test_returns_none_when_no_git(self, tmp_path):
        subdir = tmp_path / "project"
        subdir.mkdir()
        assert _find_git_root(str(subdir)) is None

    def test_finds_real_repo(self):
        import os
        import pathlib

        src = os.path.dirname(__file__)
        result = _find_git_root(src)
        if result is None:
            pytest.skip(".git not present in this test environment (e.g. sdist/wheel install)")
        assert (pathlib.Path(result) / ".git").exists()


# ---------------------------------------------------------------------------
# _get_git_snapshot
# ---------------------------------------------------------------------------
_FAKE_SHA = "abc123def456abc123def456abc123def456abc12"


class TestGetGitSnapshot:
    @patch("openlineage.client.utils.subprocess.run")
    def test_parses_sha_branch_and_tag(self, mock_run):
        mock_run.return_value = MagicMock(stdout=f"{_FAKE_SHA}|HEAD -> main, tag: v1.0.0, origin/main\n")
        sha, branch, tag = _get_git_snapshot()
        assert sha == _FAKE_SHA
        assert branch == "main"
        assert tag == "v1.0.0"

    @patch("openlineage.client.utils.subprocess.run")
    def test_branch_only_no_tag(self, mock_run):
        mock_run.return_value = MagicMock(stdout=f"{_FAKE_SHA}|HEAD -> feature/my-branch\n")
        sha, branch, tag = _get_git_snapshot()
        assert sha == _FAKE_SHA
        assert branch == "feature/my-branch"
        assert tag is None

    @patch("openlineage.client.utils.subprocess.run")
    def test_detached_head_no_branch(self, mock_run):
        mock_run.return_value = MagicMock(stdout=f"{_FAKE_SHA}|HEAD\n")
        sha, branch, tag = _get_git_snapshot()
        assert sha == _FAKE_SHA
        assert branch is None
        assert tag is None

    @patch("openlineage.client.utils.subprocess.run")
    def test_detached_head_with_tag(self, mock_run):
        mock_run.return_value = MagicMock(stdout=f"{_FAKE_SHA}|HEAD, tag: v2.0.0\n")
        sha, branch, tag = _get_git_snapshot()
        assert branch is None
        assert tag == "v2.0.0"

    @patch("openlineage.client.utils.subprocess.run")
    def test_empty_decorations(self, mock_run):
        mock_run.return_value = MagicMock(stdout=f"{_FAKE_SHA}|\n")
        sha, branch, tag = _get_git_snapshot()
        assert sha == _FAKE_SHA
        assert branch is None
        assert tag is None

    @patch("openlineage.client.utils.subprocess.run")
    def test_first_tag_wins_when_multiple(self, mock_run):
        mock_run.return_value = MagicMock(stdout=f"{_FAKE_SHA}|HEAD -> main, tag: v1.0.0, tag: v1.0.0-rc1\n")
        _, _, tag = _get_git_snapshot()
        assert tag == "v1.0.0"

    @patch("openlineage.client.utils.subprocess.run")
    def test_returns_none_tuple_on_git_failure(self, mock_run):
        mock_run.side_effect = subprocess.CalledProcessError(128, "git")
        assert _get_git_snapshot() == (None, None, None)

    @patch("openlineage.client.utils.subprocess.run")
    def test_returns_none_tuple_on_timeout(self, mock_run):
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="git", timeout=3)
        assert _get_git_snapshot() == (None, None, None)


# ---------------------------------------------------------------------------
# get_git_repo_url
# ---------------------------------------------------------------------------
class TestGetGitRepoUrl:
    def test_explicit_url_returned_as_is(self):
        assert get_git_repo_url(repo_url="https://github.com/org/repo") == "https://github.com/org/repo"

    def test_explicit_url_with_git_suffix_preserved(self):
        assert (
            get_git_repo_url(repo_url="https://github.com/org/repo.git") == "https://github.com/org/repo.git"
        )

    def test_explicit_ssh_url_with_git_suffix_preserved(self):
        assert get_git_repo_url(repo_url="git@github.com:org/repo.git") == "git@github.com:org/repo.git"

    def test_explicit_ssh_url_no_suffix(self):
        assert get_git_repo_url(repo_url="git@github.com:org/repo") == "git@github.com:org/repo"

    @patch("openlineage.client.utils.subprocess.run")
    def test_autodetect_from_git(self, mock_run):
        mock_run.return_value = MagicMock(stdout="https://github.com/org/repo.git\n")
        assert get_git_repo_url() == "https://github.com/org/repo.git"
        mock_run.assert_called_once()

    @patch("openlineage.client.utils.subprocess.run")
    def test_autodetect_strips_whitespace(self, mock_run):
        mock_run.return_value = MagicMock(stdout="  git@github.com:org/repo.git  \n")
        assert get_git_repo_url() == "git@github.com:org/repo.git"

    @patch("openlineage.client.utils.subprocess.run")
    def test_returns_none_when_git_fails(self, mock_run):
        mock_run.side_effect = subprocess.CalledProcessError(128, "git")
        assert get_git_repo_url() is None

    @patch("openlineage.client.utils.subprocess.run")
    def test_returns_none_when_git_times_out(self, mock_run):
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="git", timeout=5)
        assert get_git_repo_url() is None

    def test_returns_none_with_no_url_and_no_git(self):
        with patch("openlineage.client.utils.subprocess.run", side_effect=FileNotFoundError):
            assert get_git_repo_url() is None

    def test_explicit_empty_string_falls_through_to_git(self):
        with patch("openlineage.client.utils.subprocess.run", side_effect=FileNotFoundError):
            assert get_git_repo_url(repo_url="") is None

    def test_none_url_falls_through_to_git(self):
        with patch(
            "openlineage.client.utils.subprocess.run",
            return_value=MagicMock(stdout="https://gitlab.com/team/project.git\n"),
        ):
            assert get_git_repo_url(repo_url=None) == "https://gitlab.com/team/project.git"


# ---------------------------------------------------------------------------
# Config parsing
# ---------------------------------------------------------------------------
class TestSourceCodeLocationConfig:
    def test_default_config(self):
        config = OpenLineageConfig.from_dict({})
        assert config.facets.source_code_location.disabled is True
        assert config.facets.source_code_location.repo_url is None
        assert config.facets.source_code_location.version is None
        assert config.facets.source_code_location.branch is None
        assert config.facets.source_code_location.tag is None

    def test_config_from_dict(self):
        config = OpenLineageConfig.from_dict(
            {
                "facets": {
                    "source_code_location": {"repo_url": "https://github.com/org/repo", "disabled": True}
                }
            }
        )
        assert config.facets.source_code_location.repo_url == "https://github.com/org/repo"
        assert config.facets.source_code_location.disabled is True

    def test_config_does_not_mutate_input(self):
        params = {"facets": {"source_code_location": {"repo_url": "https://github.com/org/repo"}}}
        OpenLineageConfig.from_dict(params)
        assert "source_code_location" in params["facets"]


# ---------------------------------------------------------------------------
# add_source_code_location_facet
# ---------------------------------------------------------------------------
def _make_client(repo_url=None, disabled=False, version=None, branch=None, tag=None):
    """Create a client with noop transport and SCL config."""
    scl_config = SourceCodeLocationConfig(
        disabled=disabled, repo_url=repo_url, version=version, branch=branch, tag=tag
    )
    facets_config = FacetsConfig(source_code_location=scl_config)
    client = OpenLineageClient.__new__(OpenLineageClient)
    client._config = OpenLineageConfig(facets=facets_config)
    client.transport = NoopTransport(NoopConfig())
    client._filters = []
    return client


def _make_run_event(job_facets=None):
    return RunEvent(
        eventType=RunState.START,
        eventTime="2024-01-01T00:00:00Z",
        run=Run(runId=str(generate_new_uuid())),
        job=Job(namespace="test", name="test-job", facets=job_facets),
        producer="test",
        schemaURL="https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
    )


def _make_job_event(job_facets=None):
    return JobEvent(
        eventTime="2024-01-01T00:00:00Z",
        job=Job(namespace="test", name="test-job", facets=job_facets),
        producer="test",
        schemaURL="https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent",
    )


class TestAddSourceCodeLocationFacet:
    def test_adds_facet_to_run_event(self):
        client = _make_client(repo_url="https://github.com/org/repo")
        event = _make_run_event()
        result = client.add_source_code_location_facet(event)
        assert "sourceCodeLocation" in result.job.facets
        facet = result.job.facets["sourceCodeLocation"]
        assert facet.type == "git"
        assert facet.url == "https://github.com/org/repo"
        assert facet.repoUrl == "https://github.com/org/repo"

    def test_skips_job_event(self):
        client = _make_client(repo_url="https://github.com/org/repo")
        event = _make_job_event()
        result = client.add_source_code_location_facet(event)
        assert result.job.facets is None or "sourceCodeLocation" not in (result.job.facets or {})

    def test_skips_when_disabled(self):
        client = _make_client(repo_url="https://github.com/org/repo", disabled=True)
        event = _make_run_event()
        result = client.add_source_code_location_facet(event)
        assert result.job.facets is None or "sourceCodeLocation" not in (result.job.facets or {})

    def test_skips_when_already_present(self):
        from openlineage.client.facet_v2 import source_code_location_job

        existing = source_code_location_job.SourceCodeLocationJobFacet(
            type="git", url="https://custom.example.com/repo", repoUrl="https://custom.example.com/repo"
        )
        client = _make_client(repo_url="https://github.com/org/repo")
        event = _make_run_event(job_facets={"sourceCodeLocation": existing})
        result = client.add_source_code_location_facet(event)
        assert result.job.facets["sourceCodeLocation"].url == "https://custom.example.com/repo"

    def test_skips_when_no_url(self):
        client = _make_client(repo_url=None)
        with patch("openlineage.client.utils.subprocess.run", side_effect=FileNotFoundError):
            result = client.add_source_code_location_facet(_make_run_event())
        assert result.job.facets is None or "sourceCodeLocation" not in (result.job.facets or {})

    def test_git_info_is_cached(self):
        client = _make_client()
        with (
            patch("openlineage.client.client._find_git_root", return_value="/fake/repo"),
            patch(
                "openlineage.client.utils.subprocess.run",
                return_value=MagicMock(stdout="abc123|HEAD -> main\n"),
            ) as mock_run,
        ):
            _ = client._source_code_location
            _ = client._source_code_location
            # Two subprocess calls on first access: one git log (sha/branch/tag) +
            # one git remote get-url origin (URL). Second access hits the cache.
            assert mock_run.call_count == 2

    def test_preserves_git_suffix_in_facet(self):
        client = _make_client(repo_url="git@github.com:org/repo.git")
        event = _make_run_event()
        result = client.add_source_code_location_facet(event)
        assert result.job.facets["sourceCodeLocation"].url == "git@github.com:org/repo.git"

    def test_autodetects_version_branch_tag(self):
        client = _make_client(repo_url="https://github.com/org/repo")

        with (
            patch("openlineage.client.client._find_git_root", return_value="/fake/repo"),
            patch(
                "openlineage.client.utils.subprocess.run",
                return_value=MagicMock(stdout="abc123|HEAD -> main, tag: v1.0.0\n"),
            ),
        ):
            result = client.add_source_code_location_facet(_make_run_event())

        facet = result.job.facets["sourceCodeLocation"]
        assert facet.version == "abc123"
        assert facet.branch == "main"
        assert facet.tag == "v1.0.0"

    def test_explicit_version_branch_tag_override(self):
        client = _make_client(
            repo_url="https://github.com/org/repo",
            version="deadbeef",
            branch="feature/my-branch",
            tag="v2.0.0",
        )
        result = client.add_source_code_location_facet(_make_run_event())
        facet = result.job.facets["sourceCodeLocation"]
        assert facet.version == "deadbeef"
        assert facet.branch == "feature/my-branch"
        assert facet.tag == "v2.0.0"

    def test_tag_is_none_when_no_exact_match(self):
        client = _make_client(repo_url="https://github.com/org/repo")

        # Decorations without a tag entry → tag should remain None
        with (
            patch("openlineage.client.client._find_git_root", return_value="/fake/repo"),
            patch(
                "openlineage.client.utils.subprocess.run",
                return_value=MagicMock(stdout="abc123|HEAD -> main\n"),
            ),
        ):
            result = client.add_source_code_location_facet(_make_run_event())

        assert result.job.facets["sourceCodeLocation"].tag is None

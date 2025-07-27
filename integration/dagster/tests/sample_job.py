# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Sample job for testing OpenLineage Dagster integration."""

from dagster import job, op


@op
def test_op():
    """A simple test op that does nothing."""
    return "test result"


@job
def test_job():
    """A simple test job containing the test_op."""
    test_op()

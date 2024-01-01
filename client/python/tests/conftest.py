# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from pathlib import Path

import pytest
from openlineage.client import set_producer


@pytest.fixture(scope="session")
def root() -> Path:
    return Path(__file__).parent


@pytest.fixture(scope="session", autouse=True)
def _setup_producer() -> None:
    set_producer("https://github.com/OpenLineage/OpenLineage/tree/0.0.1/client/python")

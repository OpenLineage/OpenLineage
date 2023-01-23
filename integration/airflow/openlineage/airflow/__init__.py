# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# -*- coding: utf-8 -*-
import logging

from openlineage.airflow.version import __version__
from pkg_resources import parse_version

from airflow.version import version as AIRFLOW_VERSION

__author__ = """OpenLineage"""

if parse_version(AIRFLOW_VERSION) < parse_version("2.0.0"):     # type: ignore
    logging.warning(
        f'''
        OpenLineage support for Airflow version {AIRFLOW_VERSION} is REMOVED.
        Please make sure to upgrade your Airflow version to minimum of 2.0.0
        in order to continue using OpenLineage.
        '''
    )

from airflow.models import DAG

__all__ = ["DAG", "__version__"]

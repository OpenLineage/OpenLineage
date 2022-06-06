# SPDX-License-Identifier: Apache-2.0.
#
# -*- coding: utf-8 -*-
from pkg_resources import parse_version
from airflow.version import version as AIRFLOW_VERSION
import logging

__author__ = """OpenLineage"""
__version__ = "0.10.0"

if parse_version(AIRFLOW_VERSION) < parse_version("2.0.0"):     # type: ignore
    from openlineage.airflow.dag import DAG
    __all__ = ["DAG"]
    logging.warning(
        f'''
        OpenLineage supprot for Airflow version {AIRFLOW_VERSION}
        is DEPRECATED, and will be desupported on September 30, 2022.
        Please make sure to upgrade your Airflow version to minimum of 2.0.0
        in order to continue using OpenLineage.
        '''
    )

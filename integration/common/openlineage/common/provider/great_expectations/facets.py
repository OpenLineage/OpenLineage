# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, List, Optional, Union

import attr
from openlineage.client.facet import BaseFacet
from openlineage.common.provider.great_expectations.results import (
    GreatExpectationsAssertion,
)

from great_expectations.core import IDDict
from great_expectations.core.batch import BatchDefinition, BatchMarkers
from great_expectations.core.id_dict import BatchKwargs, BatchSpec


@attr.s
class GreatExpectationsRunFacet(BaseFacet):
    """
    Custom facet which describes the instance of GreatExpectations and the suite configuration
    """

    great_expectations_version = attr.ib()
    expectation_suite_name: str = attr.ib()
    run_id: Dict = attr.ib()  # type: ignore
    expectation_suite_meta: Dict = attr.ib()
    validation_time: str = attr.ib()
    batch_spec: Optional[BatchSpec] = attr.ib(default=None)
    batch_markers: Optional[BatchMarkers] = attr.ib(default=None)
    batch_kwargs: Optional[BatchKwargs] = attr.ib(default=None)
    active_batch_definition: Union[None, IDDict, BatchDefinition] = attr.ib(default=None)
    batch_parameters = attr.ib(default=None)
    checkpoint_name: Optional[str] = attr.ib(default=None)
    validation_id: Optional[str] = attr.ib(default=None)
    checkpoint_id: Optional[str] = attr.ib(default=None)

    @staticmethod
    def _get_schema() -> str:
        return "https://github.com/OpenLineage/OpenLineage/tree/main/integration/common/openlineage/common/provider/ge-run-facet.json"


@attr.s
class GreatExpectationsAssertionsDatasetFacet(BaseFacet):
    """
    This facet represents passed/failed status of asserted expectations on dataset
    """

    assertions: List[GreatExpectationsAssertion] = attr.ib()

    @staticmethod
    def _get_schema() -> str:
        return "https://github.com/OpenLineage/OpenLineage/tree/main/integration/common/openlineage/common/provider/ge-assertions-dataset-facet.json"

# Copyright 2018-2025 contributors to the OpenLineage project
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


@attr.define
class GreatExpectationsRunFacet(BaseFacet):
    """
    Custom facet which describes the instance of GreatExpectations and the suite configuration
    """

    great_expectations_version = attr.field()
    expectation_suite_name: str = attr.field()
    run_id: Dict = attr.field()  # type: ignore
    expectation_suite_meta: Dict = attr.field()
    validation_time: str = attr.field()
    batch_spec: Optional[BatchSpec] = attr.field(default=None)
    batch_markers: Optional[BatchMarkers] = attr.field(default=None)
    batch_kwargs: Optional[BatchKwargs] = attr.field(default=None)
    active_batch_definition: Union[None, IDDict, BatchDefinition] = attr.field(default=None)
    batch_parameters = attr.field(default=None)
    checkpoint_name: Optional[str] = attr.field(default=None)
    validation_id: Optional[str] = attr.field(default=None)
    checkpoint_id: Optional[str] = attr.field(default=None)

    @staticmethod
    def _get_schema() -> str:
        return "https://github.com/OpenLineage/OpenLineage/tree/main/integration/common/openlineage/common/provider/ge-run-facet.json"


@attr.define
class GreatExpectationsAssertionsDatasetFacet(BaseFacet):
    """
    This facet represents passed/failed status of asserted expectations on dataset
    """

    assertions: List[GreatExpectationsAssertion]

    @staticmethod
    def _get_schema() -> str:
        return "https://github.com/OpenLineage/OpenLineage/tree/main/integration/common/openlineage/common/provider/ge-assertions-dataset-facet.json"

# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0


import attr
from openlineage.client.facet import BaseFacet
from openlineage.common.provider.great_expectations.results import (
    GreatExpectationsAssertion,
)

from great_expectations.core import IDDict
from great_expectations.core.batch import BatchMarkers

try:
    from great_expectations.core.batch import (
        LegacyBatchDefinition as BatchDefinition,  # type: ignore[attr-defined]
    )
except ImportError:
    # For great-expectations < 0.18.x
    from great_expectations.core.batch import BatchDefinition  # type: ignore[no-redef, attr-defined]

from great_expectations.core.id_dict import BatchKwargs, BatchSpec


@attr.define
class GreatExpectationsRunFacet(BaseFacet):
    """
    Custom facet which describes the instance of GreatExpectations and the suite configuration
    """

    great_expectations_version = attr.field()
    expectation_suite_name: str = attr.field()
    run_id: dict = attr.field()  # type: ignore
    expectation_suite_meta: dict = attr.field()
    validation_time: str = attr.field()
    batch_spec: BatchSpec | None = attr.field(default=None)
    batch_markers: BatchMarkers | None = attr.field(default=None)
    batch_kwargs: BatchKwargs | None = attr.field(default=None)
    active_batch_definition: None | IDDict | BatchDefinition = attr.field(default=None)
    batch_parameters = attr.field(default=None)
    checkpoint_name: str | None = attr.field(default=None)
    validation_id: str | None = attr.field(default=None)
    checkpoint_id: str | None = attr.field(default=None)

    @staticmethod
    def _get_schema() -> str:
        return "https://github.com/OpenLineage/OpenLineage/tree/main/integration/common/openlineage/common/provider/ge-run-facet.json"


@attr.define
class GreatExpectationsAssertionsDatasetFacet(BaseFacet):
    """
    This facet represents passed/failed status of asserted expectations on dataset
    """

    assertions: list[GreatExpectationsAssertion]

    @staticmethod
    def _get_schema() -> str:
        return "https://github.com/OpenLineage/OpenLineage/tree/main/integration/common/openlineage/common/provider/ge-assertions-dataset-facet.json"

# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from openlineage.client.dataset import DatasetConfig
from openlineage.client.dataset.trimmers import (
    DateTrimmer,
    KeyValueTrimmer,
    MultiDirDateTrimmer,
    YearMonthTrimmer,
)


class CustomTrimmer(KeyValueTrimmer):
    pass


class SomeClass:
    pass


DEFAULT_TRIMMERS = [KeyValueTrimmer, DateTrimmer, MultiDirDateTrimmer, YearMonthTrimmer]


def test_normalization_disabled_by_default():
    config = DatasetConfig()
    assert not config.normalization_enabled


def test_default_trimmers():
    config = DatasetConfig()

    assert DEFAULT_TRIMMERS == [type(t) for t in config.get_dataset_name_trimmers()]


def test_disabling_trimmers():
    config = DatasetConfig(
        disabled_trimmers=[
            "openlineage.client.dataset.trimmers.KeyValueTrimmer",
            "openlineage.client.dataset.trimmers.MultiDirDateTrimmer",
        ]
    )

    assert [
        DateTrimmer,
        YearMonthTrimmer,
    ] == [type(t) for t in config.get_dataset_name_trimmers()]


def test_adding_extra_trimmer():
    config = DatasetConfig(extra_trimmers=["tests.dataset.test_config.CustomTrimmer"])

    assert [*DEFAULT_TRIMMERS, CustomTrimmer] == [type(t) for t in config.get_dataset_name_trimmers()]


def test_cannot_add_nonexisting_trimmer():
    config = DatasetConfig(extra_trimmers=["tests.dataset.test_config.NonexistingTrimmer"])

    assert DEFAULT_TRIMMERS == [type(t) for t in config.get_dataset_name_trimmers()]


def test_added_trimmers_must_be_actual_trimmers():
    config = DatasetConfig(extra_trimmers=["tests.dataset.test_config.SomeClass"])

    assert DEFAULT_TRIMMERS == [type(t) for t in config.get_dataset_name_trimmers()]


def test_no_duplicate_trimmers():
    config = DatasetConfig(extra_trimmers=["openlineage.client.dataset.trimmers.KeyValueTrimmer"])

    assert [*DEFAULT_TRIMMERS] == [type(t) for t in config.get_dataset_name_trimmers()]

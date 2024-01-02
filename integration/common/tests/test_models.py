# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from openlineage.common.sql import DbTableMeta


def test_eq_table_name():
    assert DbTableMeta("public.discounts") == DbTableMeta("public.discounts")
    assert DbTableMeta("discounts") != DbTableMeta("public.discounts")
    assert DbTableMeta("discounts").qualified_name == "discounts"
    assert DbTableMeta("public.discounts").qualified_name == "public.discounts"

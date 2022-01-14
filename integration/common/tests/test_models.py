# SPDX-License-Identifier: Apache-2.0

from openlineage.common.models import DbTableName


def test_eq_table_name():
    assert DbTableName('discounts') != DbTableName('public.discounts')
    assert DbTableName('discounts').qualified_name is None
    assert DbTableName('public.discounts').qualified_name == 'public.discounts'

# SPDX-License-Identifier: Apache-2.0.
from openlineage.common.models import DbTableMeta


def test_eq_table_name():
    assert DbTableMeta('discounts') != DbTableMeta('public.discounts')
    assert DbTableMeta('discounts').qualified_name is None
    assert DbTableMeta('public.discounts').qualified_name == 'public.discounts'

# SPDX-License-Identifier: Apache-2.0

import logging

import pytest
from openlineage.common.models import DbTableName
from openlineage.common.sql import SqlParser

log = logging.getLogger(__name__)


def test_parse_simple_select():
    sql_meta = SqlParser.parse(
        '''
        SELECT *
          FROM table0;
        '''
    )

    log.debug("sqlparser.parse() successful.")
    assert sql_meta.in_tables == [DbTableName('table0')]
    assert sql_meta.out_tables == []


def test_parse_simple_select_with_table_schema_prefix():
    sql_meta = SqlParser.parse(
        '''
        SELECT *
          FROM schema0.table0;
        '''
    )

    assert sql_meta.in_tables == [DbTableName('schema0.table0')]
    assert sql_meta.out_tables == []


def test_parse_simple_select_with_table_schema_prefix_and_extra_whitespace():
    sql_meta = SqlParser.parse(
        '''
        SELECT *
          FROM    schema0.table0   ;
        '''
    )

    assert sql_meta.in_tables == [DbTableName('schema0.table0')]
    assert sql_meta.out_tables == []


def test_parse_simple_select_into():
    sql_meta = SqlParser.parse(
        '''
        SELECT *
          INTO table0
          FROM table1;
        '''
    )

    assert sql_meta.in_tables == [DbTableName('table1')]
    assert sql_meta.out_tables == [DbTableName('table0')]


def test_parse_simple_join():
    sql_meta = SqlParser.parse(
        '''
        SELECT col0, col1, col2
          FROM table0
          JOIN table1
            ON t1.col0 = t2.col0
        '''
    )

    assert set(sql_meta.in_tables) == {DbTableName('table0'), DbTableName('table1')}
    assert sql_meta.out_tables == []


def test_parse_simple_inner_join():
    sql_meta = SqlParser.parse(
        '''
        SELECT col0, col1, col2
          FROM table0
         INNER JOIN table1
            ON t1.col0 = t2.col0
        '''
    )

    assert set(sql_meta.in_tables) == {DbTableName('table0'), DbTableName('table1')}
    assert sql_meta.out_tables == []


def test_parse_simple_left_join():
    sql_meta = SqlParser.parse(
        '''
        SELECT col0, col1, col2
          FROM table0
          LEFT JOIN table1
            ON t1.col0 = t2.col0
        '''
    )

    assert set(sql_meta.in_tables) == {DbTableName('table0'), DbTableName('table1')}
    assert sql_meta.out_tables == []


def test_parse_simple_left_outer_join():
    sql_meta = SqlParser.parse(
        '''
        SELECT col0, col1, col2
          FROM table0
          LEFT OUTER JOIN table1
            ON t1.col0 = t2.col0
        '''
    )

    assert set(sql_meta.in_tables) == {DbTableName('table0'), DbTableName('table1')}
    assert sql_meta.out_tables == []


def test_parse_simple_right_join():
    sql_meta = SqlParser.parse(
        '''
        SELECT col0, col1, col2
          FROM table0
          RIGHT JOIN table1
            ON t1.col0 = t2.col0;
        '''
    )

    assert set(sql_meta.in_tables) == {DbTableName('table0'), DbTableName('table1')}
    assert sql_meta.out_tables == []


def test_parse_simple_right_outer_join():
    sql_meta = SqlParser.parse(
        '''
        SELECT col0, col1, col2
          FROM table0
          RIGHT OUTER JOIN table1
            ON t1.col0 = t2.col0;
        '''
    )

    assert set(sql_meta.in_tables) == {DbTableName('table0'), DbTableName('table1')}
    assert sql_meta.out_tables == []


def test_parse_simple_insert_into():
    sql_meta = SqlParser.parse(
        '''
        INSERT INTO table0 (col0, col1, col2)
        VALUES (val0, val1, val2);
        '''
    )

    assert sql_meta.in_tables == []
    assert sql_meta.out_tables == [DbTableName('table0')]


def test_parse_simple_insert_into_select():
    sql_meta = SqlParser.parse(
        '''
        INSERT INTO table1 (col0, col1, col2)
        SELECT col0, col1, col2
          FROM table0;
        '''
    )

    assert sql_meta.in_tables == [DbTableName('table0')]
    assert sql_meta.out_tables == [DbTableName('table1')]


def test_parse_simple_cte():
    sql_meta = SqlParser.parse(
        '''
        WITH sum_trans as (
            SELECT user_id, COUNT(*) as cnt, SUM(amount) as balance
            FROM transactions
            WHERE created_date > '2020-01-01'
            GROUP BY user_id
        )
        INSERT INTO potential_fraud (user_id, cnt, balance)
        SELECT user_id, cnt, balance
          FROM sum_trans
          WHERE count > 1000 OR balance > 100000;
        '''
    )
    assert sql_meta.in_tables == [DbTableName('transactions')]
    assert sql_meta.out_tables == [DbTableName('potential_fraud')]


def test_parse_bugged_cte():
    with pytest.raises(RuntimeError):
        SqlParser.parse(
            '''
            WITH sum_trans (
                SELECT user_id, COUNT(*) as cnt, SUM(amount) as balance
                FROM transactions
                WHERE created_date > '2020-01-01'
                GROUP BY user_id
            )
            INSERT INTO potential_fraud (user_id, cnt, balance)
            SELECT user_id, cnt, balance
              FROM sum_trans
              WHERE count > 1000 OR balance > 100000;
            '''
        )


def test_parse_recursive_cte():
    sql_meta = SqlParser.parse(
        '''
        WITH RECURSIVE subordinates AS
            (SELECT employee_id,
                manager_id,
                full_name
            FROM employees
            WHERE employee_id = 2
            UNION SELECT e.employee_id,
                e.manager_id,
                e.full_name
            FROM employees e
            INNER JOIN subordinates s ON s.employee_id = e.manager_id)
        INSERT INTO sub_employees (employee_id, manager_id, full_name)
        SELECT employee_id, manager_id, full_name FROM subordinates;
        '''
    )
    assert sql_meta.in_tables == [DbTableName('employees')]
    assert sql_meta.out_tables == [DbTableName('sub_employees')]


@pytest.mark.skip(reason="no support for this right now")
def test_multiple_ctes():
    sql_meta = SqlParser.parse('''
    WITH customers AS (
            SELECT * FROM DEMO_DB.public.stg_customers
        ),
        orders AS (
            SELECT * FROM DEMO_DB.public.stg_orders
        )
    SELECT *
    FROM customers c
    JOIN orders o
    ON c.id = o.customer_id
    ''')
    assert sql_meta.in_tables == [
        DbTableName('DEMO_DB.public.stg_customers'),
        DbTableName('DEMO_DB.public.stg_orders')
    ]


def test_parse_default_schema():
    sql_meta = SqlParser.parse(
        '''
        SELECT col0, col1, col2
          FROM table0
        ''',
        'public'
    )
    assert sql_meta.in_tables == [DbTableName('public.table0')]


def test_ignores_default_schema_when_non_default_schema():
    sql_meta = SqlParser.parse(
        '''
        SELECT col0, col1, col2
          FROM transactions.table0
        ''',
        'public'
    )
    assert sql_meta.in_tables == [DbTableName('transactions.table0')]


def test_parser_integration():
    sql_meta = SqlParser.parse(
        """
        INSERT INTO popular_orders_day_of_week (order_day_of_week, order_placed_on,orders_placed)
            SELECT EXTRACT(ISODOW FROM order_placed_on) AS order_day_of_week,
                   order_placed_on,
                   COUNT(*) AS orders_placed
              FROM top_delivery_times
             GROUP BY order_placed_on;
        """,
        "public"
    )
    assert sql_meta.in_tables == [DbTableName('public.top_delivery_times')]


def test_bigquery_escaping():
    sql_meta = SqlParser.parse(
        "select * from `random-project`.`dbt_test1`.`source_table` where id = 1",
        "public"
    )
    assert sql_meta.in_tables == [DbTableName('random-project.dbt_test1.source_table')]


def test_parse_multi_statement():
    sql_meta = SqlParser.parse(
        """
        DROP TABLE IF EXISTS schema1.table1;
        CREATE TABLE schema1.table1(
          col0 VARCHAR(64),
          col1 VARCHAR(64)
        )
        DISTKEY(a)
        INTERLEAVED SORTKEY(col0,col1);
        INSERT INTO schema1.table1(col0, col1)
          SELECT col0, col1
            FROM schema0.table0;
        """
    )
    assert sql_meta.in_tables == [DbTableName('schema0.table0')]
    assert sql_meta.out_tables == [DbTableName('schema1.table1')]

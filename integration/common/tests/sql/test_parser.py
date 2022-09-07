# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging

import pytest
from openlineage.common.sql import parse, DbTableMeta, provider

log = logging.getLogger(__name__)


def test_parse_simple_select():
    sql_meta = parse(
        '''
        SELECT *
          FROM table0;
        '''
    )

    log.debug("parse() successful.")
    assert sql_meta.in_tables[0].qualified_name == DbTableMeta('table0').qualified_name


def test_parse_simple_select_with_table_schema_prefix():
    sql_meta = parse(
        '''
        SELECT *
          FROM schema0.table0;
        '''
    )

    assert sql_meta.in_tables == [DbTableMeta('schema0.table0')]
    assert sql_meta.out_tables == []


def test_parse_simple_select_with_table_schema_prefix_and_extra_whitespace():
    sql_meta = parse(
        '''
        SELECT *
          FROM    schema0.table0   ;
        '''
    )

    assert sql_meta.in_tables == [DbTableMeta('schema0.table0')]
    assert sql_meta.out_tables == []


def test_parse_simple_select_into():
    sql_meta = parse(
        '''
        SELECT *
          INTO table0
          FROM table1;
        '''
    )

    assert sql_meta.in_tables == [DbTableMeta('table1')]
    assert sql_meta.out_tables == [DbTableMeta('table0')]


def test_parse_simple_join():
    sql_meta = parse(
        '''
        SELECT col0, col1, col2
          FROM table0
          JOIN table1
            ON t1.col0 = t2.col0
        '''
    )

    assert set(sql_meta.in_tables) == {DbTableMeta('table0'), DbTableMeta('table1')}
    assert sql_meta.out_tables == []


def test_parse_comma_join():
    sql_meta = parse(
        '''
        SELECT col0, col1, col2
          FROM table0, table1
        '''
    )

    assert set(sql_meta.in_tables) == {DbTableMeta('table0'), DbTableMeta('table1')}
    assert sql_meta.out_tables == []


def test_parse_simple_inner_join():
    sql_meta = parse(
        '''
        SELECT col0, col1, col2
          FROM table0
         INNER JOIN table1
            ON t1.col0 = t2.col0
        '''
    )

    assert set(sql_meta.in_tables) == {DbTableMeta('table0'), DbTableMeta('table1')}
    assert sql_meta.out_tables == []


def test_parse_simple_left_join():
    sql_meta = parse(
        '''
        SELECT col0, col1, col2
          FROM table0
          LEFT JOIN table1
            ON t1.col0 = t2.col0
        '''
    )

    assert set(sql_meta.in_tables) == {DbTableMeta('table0'), DbTableMeta('table1')}
    assert sql_meta.out_tables == []


def test_parse_simple_left_outer_join():
    sql_meta = parse(
        '''
        SELECT col0, col1, col2
          FROM table0
          LEFT OUTER JOIN table1
            ON t1.col0 = t2.col0
        '''
    )

    assert set(sql_meta.in_tables) == {DbTableMeta('table0'), DbTableMeta('table1')}
    assert sql_meta.out_tables == []


def test_parse_simple_right_join():
    sql_meta = parse(
        '''
        SELECT col0, col1, col2
          FROM table0
          RIGHT JOIN table1
            ON t1.col0 = t2.col0;
        '''
    )

    assert set(sql_meta.in_tables) == {DbTableMeta('table0'), DbTableMeta('table1')}
    assert sql_meta.out_tables == []


def test_parse_simple_right_outer_join():
    sql_meta = parse(
        '''
        SELECT col0, col1, col2
          FROM table0
          RIGHT OUTER JOIN table1
            ON t1.col0 = t2.col0;
        '''
    )

    assert set(sql_meta.in_tables) == {DbTableMeta('table0'), DbTableMeta('table1')}
    assert sql_meta.out_tables == []


def test_parse_simple_insert_into():
    sql_meta = parse(
        '''
        INSERT INTO table0 (col0, col1, col2)
        VALUES (val0, val1, val2);
        '''
    )

    assert sql_meta.in_tables == []
    assert sql_meta.out_tables == [DbTableMeta('table0')]


def test_parse_snowflake_dialect_insert():
    sql_meta = parse(
        '''
        INSERT INTO test_orders (ord, str, num)
        VALUES
        (1, 'b', 15),
        (2, 'a', 21),
        (3, 'b', 7);
        ''',
        dialect="snowflake"
    )

    assert sql_meta.in_tables == []
    assert sql_meta.out_tables == [DbTableMeta('test_orders')]


def test_parse_simple_insert_into_select():
    sql_meta = parse(
        '''
        INSERT INTO table1 (col0, col1, col2)
        SELECT col0, col1, col2
          FROM table0;
        '''
    )

    assert sql_meta.in_tables == [DbTableMeta('table0')]
    assert sql_meta.out_tables == [DbTableMeta('table1')]


@pytest.mark.skipif(provider() == "rust", reason="sqlparser-rs requires additional keyword")
def test_parse_simple_insert():
    sql_meta = parse(
        '''
        INSERT table0 (col0, col1, col2)
        VALUES (val0, val1, val2);
        '''
    )

    assert sql_meta.in_tables == []
    assert sql_meta.out_tables == [DbTableMeta('table0')]


@pytest.mark.skipif(provider() == "rust", reason="sqlparser-rs requires additional keyword")
def test_parse_simple_insert_select():
    sql_meta = parse(
        '''
        INSERT table1 (col0, col1, col2)
        SELECT col0, col1, col2
          FROM table0;
        '''
    )

    assert sql_meta.in_tables == [DbTableMeta('table0')]
    assert sql_meta.out_tables == [DbTableMeta('table1')]


def test_parse_simple_update():
    sql_meta = parse(
        '''
        UPDATE table0 SET col0 = val0 WHERE col1 = val1
        '''
    )

    assert sql_meta.in_tables == []
    assert sql_meta.out_tables == [DbTableMeta('table0')]


def test_parse_simple_cte():
    sql_meta = parse(
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
    assert sql_meta.in_tables == [DbTableMeta('transactions')]
    assert sql_meta.out_tables == [DbTableMeta('potential_fraud')]


def test_parse_bugged_cte():
    assert parse(
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
    ) is None


def test_parse_recursive_cte():
    sql_meta = parse(
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
    assert sql_meta.in_tables == [DbTableMeta('employees')]
    assert sql_meta.out_tables == [DbTableMeta('sub_employees')]


@pytest.mark.skipif(provider() == "python", reason="no support for this in python parser")
def test_multiple_ctes():
    sql_meta = parse('''
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
        DbTableMeta('DEMO_DB.public.stg_customers'),
        DbTableMeta('DEMO_DB.public.stg_orders')
    ]


def test_parse_default_schema():
    sql_meta = parse(
        '''
        SELECT col0, col1, col2
          FROM table0
        ''',
        default_schema='public'
    )
    assert sql_meta.in_tables == [DbTableMeta('public.table0')]


def test_ignores_default_schema_when_non_default_schema():
    sql_meta = parse(
        '''
        SELECT col0, col1, col2
          FROM transactions.table0
        ''',
        'public'
    )
    assert sql_meta.in_tables == [DbTableMeta('transactions.table0')]


def test_parser_integration():
    sql_meta = parse(
        """
        INSERT INTO popular_orders_day_of_week (order_day_of_week, order_placed_on,orders_placed)
            SELECT EXTRACT(ISODOW FROM order_placed_on) AS order_day_of_week,
                   order_placed_on,
                   COUNT(*) AS orders_placed
              FROM top_delivery_times
             GROUP BY order_placed_on;
        """,
        default_schema="public"
    )
    assert sql_meta.in_tables == [DbTableMeta('public.top_delivery_times')]


def test_bigquery_escaping():
    sql_meta = parse(
        "select * from `random-project`.`dbt_test1`.`source_table` where id = 1",
        dialect="bigquery",
        default_schema="public"
    )
    assert sql_meta.in_tables == [DbTableMeta('random-project.dbt_test1.source_table')]


@pytest.mark.skipif(provider() == "python", reason="python does not understand DDL")
def test_create_table():
    sql_meta = parse("""
        CREATE TABLE Persons (
        PersonID int,
        LastName varchar(255),
        FirstName varchar(255),
        Address varchar(255),
        City varchar(255));
        """)
    assert sql_meta.in_tables == []
    assert sql_meta.out_tables == [DbTableMeta("Persons")]


@pytest.mark.skipif(provider() == "python", reason="python does not understand DDL")
def test_create_table_if_not_exists():
    sql_meta = parse("""
    CREATE TABLE IF NOT EXISTS popular_orders_day_of_week (
      order_day_of_week VARCHAR(64) NOT NULL,
      order_placed_on   TIMESTAMP NOT NULL,
      orders_placed     INTEGER NOT NULL
    )""")
    assert sql_meta.in_tables == []
    assert sql_meta.out_tables == [DbTableMeta("popular_orders_day_of_week")]


def test_parse_multi_statement():
    sql_meta = parse(
        """
        DROP TABLE IF EXISTS schema1.table1;
        CREATE TABLE schema1.table1(
          col0 VARCHAR(64),
          col1 VARCHAR(64)
        );
        INSERT INTO schema1.table1(col0, col1)
          SELECT col0, col1
            FROM schema0.table0;
        """
    )
    assert sql_meta.in_tables == [DbTableMeta('schema0.table0')]
    assert sql_meta.out_tables == [DbTableMeta('schema1.table1')]


def test_parse_statement_list():
    sql_meta = parse(["""
        DROP TABLE IF EXISTS schema1.table1;
        CREATE TABLE schema1.table1(
          col0 VARCHAR(64),
          col1 VARCHAR(64)
        );""", """
        INSERT INTO schema1.table1(col0, col1)
          SELECT col0, col1
            FROM schema0.table0;
        """])
    assert sql_meta.in_tables == [DbTableMeta('schema0.table0')]
    assert sql_meta.out_tables == [DbTableMeta('schema1.table1')]


def test_parse_copy_into_snowflake_at_syntax():
    parse(["""
            COPY INTO SCHEMA.SOME_MONITORING_SYSTEM
            FROM (
                SELECT
                t.$1:st AS st,
                t.$1:index AS index,
                t.$1:cid AS cid,
                t.$1:k8s AS k8s,
                t.$1:cn AS cn,
                t.$1:did AS did,
                t.$1:tid AS tid,
                t.$1:tn AS tn,
                t.$1:mt AS mt,
                t.$1:op AS op,
                t.$1:drid AS drid,
                t.$1:mi AS mi,
                t.$1:q3dm17 AS q3dm17,
                t.$1:rsd AS rsd,
                t.$1:red AS red,
                t.$1:rd AS rd,
                t.$1:state AS state,
                t.$1:es AS es,
                t.$1:pool AS pool,
                t.$1:queue AS queue,
                t.$1:pw AS pw,
                metadata$fn AS load_fn,
                metadata$frn AS load_filerow,
                CURRENT_TIMESTAMP AS lts
                FROM @schema.general_finished AS t
            )"""])

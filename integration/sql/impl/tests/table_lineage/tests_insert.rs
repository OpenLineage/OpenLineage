// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::*;
use openlineage_sql::TableLineage;

#[test]
fn insert_values() {
    assert_eq!(
        test_sql("INSERT INTO TEST VALUES(1)",)
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: vec![],
            out_tables: tables(vec!["TEST"])
        }
    );
}

#[test]
fn insert_values_subquery() {
    assert_eq!(
        test_sql("INSERT INTO report (total) VALUES ((SELECT count(*) FROM orders))",)
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["orders"]),
            out_tables: tables(vec!["report"])
        }
    );
}

#[test]
fn insert_cols_values() {
    assert_eq!(
        test_sql("INSERT INTO tbl(col1, col2) VALUES (1, 2), (2, 3)",)
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: vec![],
            out_tables: tables(vec!["tbl"])
        }
    );
}

#[test]
fn insert_select_table() {
    assert_eq!(
        test_sql("INSERT INTO TEST SELECT * FROM TEMP",)
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["TEMP"]),
            out_tables: tables(vec!["TEST"])
        }
    );
}

#[test]
fn insert_select_table_2() {
    assert_eq!(
        test_sql(
            "
            insert into \"a1\".\"a2\"
            select to_date(C_AT),
                   dm.C_NAME,
                   P,
                   count(*)
            from \"b1\".\"b2\" as s
                     left join \"m\".\"dim\" as dm on (dm.m_id = s.m_id)
            where PROCESSED_AT = '2022-04-14'
            group by to_date(C_AT), dm.C_NAME, P
            ;",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["\"b1\".\"b2\"", "\"m\".\"dim\""]),
            out_tables: tables(vec!["\"a1\".\"a2\""])
        }
    )
}

#[test]
fn insert_nested_select() {
    assert_eq!(test_sql("
                INSERT INTO popular_orders_day_of_week (order_day_of_week, order_placed_on,orders_placed)
                SELECT EXTRACT(ISODOW FROM order_placed_on) AS order_day_of_week,
                    order_placed_on,
                    COUNT(*) AS orders_placed
                FROM top_delivery_times
                GROUP BY order_placed_on;
            ",
    ).unwrap().table_lineage, TableLineage {
        in_tables: tables(vec!["top_delivery_times"]),
        out_tables: tables(vec!["popular_orders_day_of_week"])
    })
}

#[test]
fn insert_snowflake_table() {
    assert_eq!(
        test_sql_dialect("\n    INSERT INTO test_orders (ord, str, num) VALUES\n    (1, 'b', 15),\n    (2, 'a', 21),\n    (3, 'b', 7);\n   ", "snowflake").unwrap().table_lineage,
        TableLineage {
            in_tables: vec![],
            out_tables: tables(vec!["test_orders"])
        }
    )
}

#[test]
fn insert_overwrite_table() {
    assert_eq!(
        test_sql_dialect(
            "\
        INSERT OVERWRITE TABLE schema.dps
        PARTITION (ds = '2022-03-30')
        SELECT
            pid,
            uid,
            pii_userid,
            NULL as session_id,
            NULL as session_start_ts,
            COUNT(1) AS session_cnt,
            SUM(
                UNIX_TIMESTAMP(stopped) - UNIX_TIMESTAMP(joined)
            ) AS time_spent_sec
        FROM schema.fpsm
        WHERE ds = '2022-03-30'
            AND UNIX_TIMESTAMP(stopped) - UNIX_TIMESTAMP(joined) BETWEEN 0 AND 28800
        GROUP BY
            pid,
            uid,
            pii_userid
    ",
            "hive"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["schema.fpsm"]),
            out_tables: tables(vec!["schema.dps"])
        }
    )
}

#[test]
fn insert_overwrite_subqueries() {
    assert_eq!(
        test_sql_dialect(
            "
        INSERT OVERWRITE TABLE mytable
        PARTITION (ds = '2022-03-31')
        SELECT
            *
        FROM
        (SELECT * FROM table2) a",
            "hive"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["table2"]),
            out_tables: tables(vec!["mytable"])
        }
    )
}

#[test]
fn insert_overwrite_multiple_subqueries() {
    assert_eq!(
        test_sql_dialect(
            "
        INSERT OVERWRITE TABLE mytable
        PARTITION (ds = '2022-03-31')
        SELECT
            *
        FROM
        (SELECT * FROM table2
         UNION
         SELECT * FROM table3
         UNION ALL
         SELECT * FROM table4) a
         ",
            "hive"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["table2", "table3", "table4"]),
            out_tables: tables(vec!["mytable"])
        }
    )
}

#[test]
fn insert_overwrite_partition() {
    assert_eq!(
        test_sql_dialect(
            "
            INSERT OVERWRITE TABLE d_d_n.g_d_t_r
            PARTITION (ds = '2022-02-24')
            SELECT
                u.u_i,
                g.d_r
            FROM
                (SELECT * FROM d_d_n.u_t_250 WHERE ds = '2022-02-24') u
            JOIN
                (SELECT * FROM d_n_p.g_d_r WHERE ds = '2022-02-24') g
            ON
                u.u_i = g.u_i
        ",
            "hive"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["d_d_n.u_t_250", "d_n_p.g_d_r"]),
            out_tables: tables(vec!["d_d_n.g_d_t_r"])
        }
    )
}

#[test]
fn set_before_insert() {
    assert_eq!(
        test_sql_dialect(
            "
                SET hive.something=false;
                INSERT INTO TABLE a.b.c VALUES (1, 2, 3);
            ",
            "hive"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: vec![],
            out_tables: tables(vec!["a.b.c"])
        }
    )
}

#[test]
fn insert_overwrite_hive_sets_large() {
    assert_eq!(
        test_sql_dialect(
            "
                SET hive.auto.convert.join=false;
                SET hive.tez.auto.reducer.parallelism=false;
                SET mapred.reduce.tasks=150;
                INSERT OVERWRITE TABLE d_d_p.d_f_s
                PARTITION (ds = '2022-05-01')
                  SELECT
                      T1.p_id,
                      T2.s_st_ts
                      FROM d_p.f_p_s AS T1
                      INNER JOIN
                        (SELECT
                           fpsm.p_id,
                           MIN(fpsm.ds) as ds
                           from d_p.f_p_s_merged  AS fpsm
                           WHERE fpsm.ds = '2022-05-01'
                           GROUP BY
                               fpsm.p_id
                        ) AS T2
                        ON T1.p_id = T2.p_id
        ",
            "hive"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["d_p.f_p_s", "d_p.f_p_s_merged"]),
            out_tables: tables(vec!["d_d_p.d_f_s"])
        }
    )
}

#[test]
fn insert_group_by() {
    assert_eq!(
        test_sql(
            "
            insert into \"a1\".\"a2\"
            select
              C_DT,
              C_NAME,
              U_C,
              U_M,
              U_SOURCE,
              P,
              IFF(U_SOURCE is NULL, 'abc', 'def') as D_TYPE,
              count(*)
            from
              \"b1\".\"b2\"
            where
              P_AT = '2022-04-14'
            group by
              C_DT,
              C_NAME,
              U_C,
              U_M,
              U_SOURCE,
              P,
              U_SOURCE;"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["\"b1\".\"b2\""]),
            out_tables: tables(vec!["\"a1\".\"a2\""])
        }
    )
}

#[test]
fn insert_nested_with_select() {
    assert_eq!(
        test_sql(
            "
        insert into \"a1\".\"a2\" with a_b_c as (
          select
            created_dt,
            s_id,
            user_id,
            match_id,
            region,
            x
          from
            (
              (
                select
                  created_dt,
                  s_id,
                  puuid as user_id,
                  cast(g_id as string) as match_id,
                  coalesce(region, '0') as region,
                  'A' as x
                from
                  \"b1\".\"b2\"
                where
                  processed_at = '2022-04-14'
              )
              union all
                (
                  select
                    created_dt,
                    s_id,
                    puuid as user_id,
                    cast(g_id as string) as match_id,
                    coalesce(region, '0') as region,
                    'B' as x
                  from
                    \"c1\".\"c2\"
                  where
                    processed_at = '2022-04-14'
                )
              union all
                (
                  select
                    created_dt,
                    s_id,
                    puuid as user_id,
                    match_id,
                    coalesce(region, '0') as region,
                    'C' as x
                  from
                    \"d1\".\"d2\"
                  where
                    processed_at = '2022-04-14'
                )
              union all
                (
                  select
                    created_dt,
                    s_id,
                    s_id as user_id,
                    -- using s_id as proxy to puuid
                    match_id,
                    coalesce(region, '0') as region,
                    'D' as x
                  from
                    \"e1\".\"e2\"
                  where
                    processed_at = '2022-04-14'
                )
              union all
                (
                  select
                    created_dt,
                    s_id,
                    user_id,
                    guid as match_id,
                    '0' as region,
                    'E' as x
                  from
                    \"f1\".\"f2\"
                  where
                    processed_at = '2022-04-14'
                )
            )
          group by
            created_dt,
            s_id,
            user_id,
            match_id,
            region,
            x
        )
        select
          created_dt,
          region,
          x,
          count(*),
          count(distinct user_id)
        from
          a_b_c
        group by
          created_dt,
          region,
          x;"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec![
                "\"b1\".\"b2\"",
                "\"c1\".\"c2\"",
                "\"d1\".\"d2\"",
                "\"e1\".\"e2\"",
                "\"f1\".\"f2\""
            ]),
            out_tables: tables(vec!["\"a1\".\"a2\""])
        }
    )
}

#[test]
fn test_multiple_statements_delete_insert() {
    assert_eq!(
        test_sql(
            "
            DELETE FROM public.\"Employees\";
            INSERT INTO public.\"Employees\" VALUES (1, 'TALES OF SHIVA', 'Mark', 'mark', 0);
        "
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: vec![],
            out_tables: tables(vec!["public.\"Employees\""]),
        }
    )
}

#[test]
fn test_multiple_statements_insert_insert() {
    assert_eq!(
        test_sql(
            "
            INSERT INTO a.a VALUES(1,2);
            INSERT INTO b.b VALUES(1,2);
        "
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: vec![],
            out_tables: tables(vec!["a.a", "b.b"]),
        }
    )
}

#[test]
fn test_triple_statements_insert_insert_insert() {
    assert_eq!(
        test_sql(
            "
            INSERT INTO a.a VALUES(1,2);
            INSERT INTO b.b SELECT * FROM a.a;
            INSERT INTO c.c VALUES(1,2);
        "
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a.a"]),
            out_tables: tables(vec!["a.a", "b.b", "c.c"]),
        }
    )
}

#[test]
fn insert_overwrite_multiple_unions() {
    assert_eq!(
        test_sql_dialect(
            "
            INSERT OVERWRITE TABLE d_d_n.a_p_s_v
            PARTITION (ds = '2022-05-01')
            SELECT
                sub.p_i,
                MIN(sub.f_a_d) AS session_id,
                MAX(sub.l_a_d) AS l_a_d
            FROM
                (
                SELECT
                    p_i,
                    f_a_d,
                    l_a_d
                FROM
                    d_d_n.d_p_s_v
                WHERE
                    ds = '2022-05-01'
                UNION ALL
                SELECT
                    p_i,
                    f_a_d,
                    l_a_d
                FROM
                    d_d_n.a_p_s_v
                WHERE
                    ds = '2022-04-30'
                    AND l_a_d >= DATE_ADD('2022-05-01', -56)
                UNION ALL
                SELECT
                    p_i,
                    f_a_d,
                    l_a_d
                FROM
                    d_d_n.a_p_s_v
                WHERE
                    ds = '2022-04-24'
                    AND l_a_d >= DATE_ADD('2022-05-01', -56)
                UNION ALL
                SELECT
                    p_i,
                    f_a_d,
                    l_a_d
                FROM
                    d_d_n.a_p_s_v
                WHERE
                    ds = '2022-04-03'
                    AND l_a_d >= DATE_ADD('2022-05-01', -56)
                ) sub
            GROUP BY
                sub.p_i,
                sub.u_i
        ",
            "hive"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["d_d_n.a_p_s_v", "d_d_n.d_p_s_v"]),
            out_tables: tables(vec!["d_d_n.a_p_s_v"])
        }
    )
}

#[test]
fn insert_overwrite_partition_dates() {
    assert_eq!(
        test_sql_dialect(
            "\
        INSERT OVERWRITE TABLE ddw.aps2
        PARTITION (ds = '2022-05-01')
        SELECT
            sub.pid,
            sub.uid,
            sub.userkey,
            MIN(sub.fs_id) AS session_id,
            MIN(sub.fs_start_ts) AS session_start_ts,
            SUM(sub.sc_1d) AS sc_1d,
            SUM(sub.sc_7d) AS sc_7d,
            SUM(sub.sc_28d) AS sc_28d,
            SUM(sub.sc_inf) AS session_cnt_inf,
            SUM(sub.tss_1d) AS tss_1d,
            SUM(sub.tss_7d) AS tss_7d,
            SUM(sub.tss_28d) AS tss_28d,
            SUM(sub.tss_inf) AS tss_inf,
            SUM(sub.datelist) AS datelist,
            MIN(sub.fad) AS fad,
            MAX(sub.lad) AS lad
        FROM
            (
            SELECT
                pid,
                uid,
                userkey,
                fs_id,
                fs_start_ts,
                session_cnt AS session_cnt_1d,
                session_cnt AS session_cnt_7d,
                session_cnt AS session_cnt_28d,
                session_cnt AS session_cnt_inf,
                tss AS tss_1d,
                tss AS tss_7d,
                tss AS tss_28d,
                tss AS tss_inf,
                1 AS datelist,
                '2022-05-01' AS fad,
                '2022-05-01' AS lad
            FROM
                ddw.dps
            WHERE
                ds = '2022-05-01'
            UNION ALL
            SELECT
                pid,
                uid,
                userkey,
                fs_id,
                fs_start_ts,
                0 AS session_cnt_1d,
                session_cnt_7d AS session_cnt_7d,
                session_cnt_28d AS session_cnt_28d,
                session_cnt_inf AS session_cnt_inf,
                0 AS tss_1d,
                tss_7d AS tss_7d,
                tss_28d AS tss_28d,
                tss_inf AS tss_inf,
                (datelist % 549755813888) * 2 AS datelist,
                fad AS fad,
                lad AS lad
            FROM
                ddw.aps2
            WHERE
                ds = '2022-04-30'
                AND lad >= DATE_ADD('2022-05-01', -56)
            UNION ALL
            SELECT
                pid,
                uid,
                userkey,
                fs_id,
                fs_start_ts,
                0 AS session_cnt_1d,
                -session_cnt_1d AS session_cnt_7d,
                0 AS session_cnt_28d,
                0 AS session_cnt_inf,
                0 AS tss_1d,
                -tss_1d AS tss_7d,
                0 AS tss_28d,
                0 AS tss_inf,
                0 AS datelist,
                fad,
                lad
            FROM
                ddw.aps2
            WHERE
                ds = '2022-04-24'
                AND lad >= DATE_ADD('2022-05-01', -56)
            UNION ALL
            SELECT
                pid,
                uid,
                userkey,
                fs_id,
                fs_start_ts,
                0 AS session_cnt_1d,
                0 AS session_cnt_7d,
                -session_cnt_1d AS session_cnt_28d,
                0 AS session_cnt_inf,
                0 AS tss_1d,
                0 AS tss_7d,
                -tss_1d AS tss_28d,
                0 AS tss_inf,
                0 AS datelist,
                fad,
                lad
            FROM
                ddw.aps2
            WHERE
                ds = '2022-04-03'
                AND lad >= DATE_ADD('2022-05-01', -56)
            ) sub
        GROUP BY
            sub.pid,
            sub.uid,
            sub.userkey
        ",
            "hive"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["ddw.aps2", "ddw.dps"]),
            out_tables: tables(vec!["ddw.aps2"])
        }
    )
}

#[test]
fn insert_all_multiple_targets() {
    assert_eq!(
        test_sql_dialect(
            "INSERT ALL INTO t1 VALUES (x) INTO t2 VALUES (y) SELECT x, y FROM s",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["s"]),
            out_tables: tables(vec!["t1", "t2"])
        }
    );
}

#[test]
fn insert_all_when_exists_predicate_table_is_input_with_cte_source() {
    // The source query defines and reads a CTE while the WHEN predicate
    // reads another table: both the CTE's own input and the predicate
    // table are inputs of the statement. (The CTE rides on the source
    // query - a WITH leading the whole INSERT ALL is not a shape the
    // parser accepts.)
    assert_eq!(
        test_sql_dialect(
            "INSERT ALL WHEN EXISTS (SELECT 1 FROM ref) THEN INTO t1 VALUES (x) \
             WITH c AS (SELECT x FROM s) SELECT x FROM c",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["ref", "s"]),
            out_tables: tables(vec!["t1"])
        }
    );
}

#[test]
fn insert_first_when_scalar_subquery_table_is_input_with_cte_source() {
    assert_eq!(
        test_sql_dialect(
            "INSERT FIRST WHEN x > (SELECT MAX(m) FROM limits) THEN INTO t1 VALUES (x) \
             ELSE INTO t2 VALUES (x) WITH c AS (SELECT x FROM s) SELECT x FROM c",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["limits", "s"]),
            out_tables: tables(vec!["t1", "t2"])
        }
    );
}

#[test]
fn insert_all_when_plain_comparison_adds_no_input_with_cte_source() {
    // Control: with a CTE source and a WHEN condition that reads no
    // table, the CTE's input is the statement's only input.
    assert_eq!(
        test_sql_dialect(
            "INSERT ALL WHEN x > 0 THEN INTO t1 VALUES (x) \
             WITH c AS (SELECT x FROM s) SELECT x FROM c",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["s"]),
            out_tables: tables(vec!["t1"])
        }
    );
}

#[test]
fn insert_first_when_else_targets() {
    assert_eq!(
        test_sql_dialect(
            "INSERT FIRST WHEN x > 0 THEN INTO t1 VALUES (x) \
             WHEN x < 0 THEN INTO t2 VALUES (x) ELSE INTO t3 VALUES (x) \
             SELECT x FROM s",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["s"]),
            out_tables: tables(vec!["t1", "t2", "t3"])
        }
    );
}

#[test]
fn insert_into_identifier_target() {
    // IDENTIFIER('t1') names the target through a function call; the raw
    // name would register a false output called IDENTIFIER.
    assert_eq!(
        test_sql_dialect("INSERT INTO IDENTIFIER('t1') SELECT * FROM s", "snowflake")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["s"]),
            out_tables: tables(vec!["t1"])
        }
    );
}

#[test]
fn insert_all_identifier_targets() {
    assert_eq!(
        test_sql_dialect(
            "INSERT ALL INTO IDENTIFIER('t1') VALUES (x) INTO IDENTIFIER('t2') VALUES (y) \
             SELECT x, y FROM s",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["s"]),
            out_tables: tables(vec!["t1", "t2"])
        }
    );
}

#[test]
fn insert_first_identifier_when_else_targets() {
    assert_eq!(
        test_sql_dialect(
            "INSERT FIRST WHEN x > 0 THEN INTO IDENTIFIER('t1') VALUES (x) \
             ELSE INTO IDENTIFIER('t3') VALUES (x) SELECT x FROM s",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["s"]),
            out_tables: tables(vec!["t1", "t3"])
        }
    );
}

#[test]
fn insert_into_identifier_qualified_target() {
    // A qualified IDENTIFIER('db.schema.t1') value is an object name, not
    // a single table name: database, schema and table must be recorded
    // separately, exactly as for a literal qualified target.
    assert_eq!(
        test_sql_dialect(
            "INSERT INTO IDENTIFIER('db.schema.t1') SELECT * FROM s",
            "snowflake"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["s"]),
            out_tables: tables(vec!["db.schema.t1"])
        }
    );
}

#[test]
fn insert_into_identifier_quoted_qualified_target() {
    // Quoted components inside the IDENTIFIER value stay quoted and exact.
    let output = test_sql_dialect(
        "INSERT INTO IDENTIFIER('\"db\".\"Schema\".\"t1\"') SELECT * FROM s",
        "snowflake",
    )
    .unwrap();
    assert_eq!(output.table_lineage.out_tables.len(), 1);
    let target = &output.table_lineage.out_tables[0];
    assert_eq!(target.database.as_deref(), Some("db"));
    assert_eq!(target.schema.as_deref(), Some("Schema"));
    assert_eq!(target.name, "t1");
}

#[test]
fn insert_into_identifier_mixed_quoted_qualified_target() {
    let output = test_sql_dialect(
        "INSERT INTO IDENTIFIER('db.\"Schema\".t1') SELECT * FROM s",
        "snowflake",
    )
    .unwrap();
    assert_eq!(output.table_lineage.out_tables.len(), 1);
    let target = &output.table_lineage.out_tables[0];
    assert_eq!(target.database.as_deref(), Some("db"));
    assert_eq!(target.schema.as_deref(), Some("Schema"));
    assert_eq!(target.name, "t1");
}

#[test]
fn insert_all_identifier_qualified_targets() {
    assert_eq!(
        test_sql_dialect(
            "INSERT ALL INTO IDENTIFIER('db.schema.t1') VALUES (x) \
             INTO IDENTIFIER('db2.schema2.t2') VALUES (y) SELECT x, y FROM s",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["s"]),
            out_tables: tables(vec!["db.schema.t1", "db2.schema2.t2"])
        }
    );
}

#[test]
fn select_from_identifier_qualified_table() {
    // resolve_effective_table_name is shared by every table factor, not
    // only insert targets: a qualified IDENTIFIER('db.schema.t') in a
    // plain SELECT resolves to the same database/schema/table
    // components as a literal qualified name, not to one table
    // literally named "db.schema.t".
    assert_eq!(
        test_sql_dialect("SELECT * FROM IDENTIFIER('db.schema.t')", "snowflake")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["db.schema.t"]),
            out_tables: tables(vec![])
        }
    );
}

#[test]
fn insert_target_named_like_cte() {
    // The target is outside the CTE's alias scope: sharing its name must
    // not suppress it as an output.
    assert_eq!(
        test_sql_dialect(
            "WITH t1 AS (SELECT * FROM s) INSERT INTO t1 SELECT * FROM t1",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["s"]),
            out_tables: tables(vec!["t1"])
        }
    );
}

#[test]
fn insert_target_named_like_cte_in_source() {
    assert_eq!(
        test_sql_dialect(
            "INSERT INTO t1 WITH t1 AS (SELECT a FROM s) SELECT * FROM t1",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["s"]),
            out_tables: tables(vec!["t1"])
        }
    );
}

#[test]
fn insert_target_with_unrelated_cte() {
    assert_eq!(
        test_sql_dialect(
            "WITH c AS (SELECT * FROM s) INSERT INTO t1 SELECT * FROM c",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["s"]),
            out_tables: tables(vec!["t1"])
        }
    );
}

#[test]
fn insert_all_does_not_abort_other_statements() {
    // A multi-table insert used to panic, aborting the whole parse call and
    // losing the lineage of every other statement in it.
    assert_eq!(
        test_multiple_sql_dialect(
            vec![
                "INSERT INTO a SELECT * FROM b",
                "INSERT ALL INTO db.schema.t1 (c1) VALUES (x) INTO t2 VALUES (y) \
                 SELECT x, y FROM s",
            ],
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["b", "s"]),
            // SqlMeta sorts outputs by (database, schema, name), so the
            // unqualified t2 sorts ahead of the qualified db.schema.t1.
            out_tables: tables(vec!["a", "t2", "db.schema.t1"])
        }
    );
}

#[test]
fn insert_all_when_exists_predicate_table_is_input() {
    // A WHEN predicate is a general expression: a table it reads through
    // a subquery is an input of the statement, like the source query's.
    assert_eq!(
        test_sql_dialect(
            "INSERT ALL WHEN EXISTS (SELECT 1 FROM ref) THEN INTO t1 VALUES (x) \
             SELECT x FROM s",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["ref", "s"]),
            out_tables: tables(vec!["t1"])
        }
    );
}

#[test]
fn insert_first_when_scalar_subquery_table_is_input() {
    assert_eq!(
        test_sql_dialect(
            "INSERT FIRST WHEN x > (SELECT MAX(m) FROM limits) THEN INTO t1 VALUES (x) \
             ELSE INTO t2 VALUES (x) SELECT x FROM s",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["limits", "s"]),
            out_tables: tables(vec!["t1", "t2"])
        }
    );
}

#[test]
fn insert_all_when_plain_comparison_adds_no_input() {
    // Control: WHEN conditions that read no table add nothing beyond the
    // source query's inputs.
    assert_eq!(
        test_sql_dialect(
            "INSERT ALL WHEN x > 0 THEN INTO t1 VALUES (x) \
             WHEN x < 0 THEN INTO t2 VALUES (x) SELECT x FROM s",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["s"]),
            out_tables: tables(vec!["t1", "t2"])
        }
    );
}

#[test]
fn insert_into_identifier_prefixed_qualified_name_mssql() {
    // A qualified target whose first part is `identifier` is a plain
    // table name - a catalog or schema called identifier - not an
    // IDENTIFIER() call: the call form carries an argument list, a plain
    // name does not.
    assert_eq!(
        test_sql_dialect("INSERT INTO identifier.dbo.t1 SELECT * FROM src", "mssql")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["src"]),
            out_tables: tables(vec!["identifier.dbo.t1"])
        }
    );
}

#[test]
fn insert_into_table_named_identifier_databricks() {
    // Same for the bare one-part name on Databricks.
    assert_eq!(
        test_sql_dialect("INSERT INTO identifier SELECT * FROM src", "databricks")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["src"]),
            out_tables: tables(vec!["identifier"])
        }
    );
}

#[test]
fn insert_into_table_named_identifier_with_columns_databricks() {
    // A genuine column list of plain names belongs to a table called
    // `identifier`, not to an IDENTIFIER(...) call: the target stands.
    assert_eq!(
        test_sql_dialect(
            "INSERT INTO identifier (a, b) SELECT * FROM src",
            "databricks",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["src"]),
            out_tables: tables(vec!["identifier"])
        }
    );
}

#[test]
fn insert_into_identifier_function_databricks() {
    // Databricks also accepts IDENTIFIER('cat.sch.t1') as the target;
    // sqlparser parses that as the bare name IDENTIFIER with the string
    // in the column list, and the target is the named table.
    assert_eq!(
        test_sql_dialect(
            "INSERT INTO IDENTIFIER('cat.sch.t1') SELECT * FROM src",
            "databricks",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["src"]),
            out_tables: tables(vec!["cat.sch.t1"])
        }
    );
}

#[test]
fn insert_all_when_cte_does_not_hide_later_when_table() {
    // A CTE defined inside one WHEN predicate is scoped to that
    // predicate: the real table q read by the second predicate stays an
    // input, alongside the first predicate's lookup table lk.
    assert_eq!(
        test_sql_dialect(
            "INSERT ALL \
             WHEN x IN (WITH q AS (SELECT x FROM lk) SELECT x FROM q) THEN INTO o1 \
             WHEN x IN (SELECT x FROM q) THEN INTO o2 \
             SELECT x FROM src",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["lk", "q", "src"]),
            out_tables: tables(vec!["o1", "o2"])
        }
    );
}

#[test]
fn insert_all_when_source_cte_shadows_same_named_table() {
    // Pinned behavior: a WHEN predicate's reference to c resolves to the
    // source query's CTE of the same name, so s is the only input. How
    // Snowflake scopes the source WITH against WHEN subqueries is not
    // modelled; the predicate reference is treated as a CTE reference.
    assert_eq!(
        test_sql_dialect(
            "INSERT ALL WHEN EXISTS (SELECT 1 FROM c) THEN INTO t1 VALUES (x) \
             WITH c AS (SELECT x FROM s) SELECT x FROM c",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["s"]),
            out_tables: tables(vec!["t1"])
        }
    );
}

#[test]
fn insert_all_target_named_like_source_cte_is_output() {
    // A multi-table target is outside the source CTE's alias scope:
    // sharing its name must not suppress it as an output, and the CTE's
    // own input is still collected.
    assert_eq!(
        test_sql_dialect(
            "INSERT ALL INTO c VALUES (x) \
             WITH c AS (SELECT x FROM s) SELECT x FROM c",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["s"]),
            out_tables: tables(vec!["c"])
        }
    );
}

#[test]
fn insert_into_identifier_variable_target_has_no_output() {
    // IDENTIFIER($t) names the target through a session variable: there
    // is no static table name to report, so no output is emitted. This
    // matches the SELECT side, where IDENTIFIER($t) yields no input.
    assert_eq!(
        test_sql_dialect("INSERT INTO IDENTIFIER($t) SELECT * FROM s", "snowflake")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["s"]),
            out_tables: tables(vec![])
        }
    );
}

#[test]
fn insert_into_identifier_bind_variable_target_has_no_output() {
    // Same for a bind variable, IDENTIFIER(:bind).
    assert_eq!(
        test_sql_dialect("INSERT INTO IDENTIFIER(:bind) SELECT * FROM s", "snowflake")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["s"]),
            out_tables: tables(vec![])
        }
    );
}

#[test]
fn insert_into_identifier_quoted_parts_target_has_no_output() {
    // IDENTIFIER("db"."t1") passes identifier parts rather than one
    // string literal; that form is not resolved, again as on the SELECT
    // side.
    assert_eq!(
        test_sql_dialect(
            "INSERT INTO IDENTIFIER(\"db\".\"t1\") SELECT * FROM s",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["s"]),
            out_tables: tables(vec![])
        }
    );
}

#[test]
fn insert_into_identifier_empty_string_has_no_output() {
    // IDENTIFIER('') names nothing: no output is emitted rather than a
    // dataset with an empty name. IDENTIFIER('""') behaves the same.
    for sql in [
        "INSERT INTO IDENTIFIER('') SELECT x FROM s",
        "INSERT INTO IDENTIFIER('\"\"') SELECT x FROM s",
    ] {
        assert_eq!(
            test_sql_dialect(sql, "snowflake").unwrap().table_lineage,
            TableLineage {
                in_tables: tables(vec!["s"]),
                out_tables: tables(vec![])
            }
        );
    }
}

#[test]
fn insert_into_identifier_empty_string_databricks_has_no_output() {
    // On Databricks the IDENTIFIER('...') argument lands in the insert
    // column list; an empty string names nothing, so no output is
    // emitted - the same check the function form applies.
    assert_eq!(
        test_sql_dialect("INSERT INTO IDENTIFIER('') SELECT * FROM s", "databricks")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["s"]),
            out_tables: tables(vec![])
        }
    );
}

#[test]
fn insert_into_identifier_blank_string_mssql_has_no_output() {
    // A blank string names nothing either.
    assert_eq!(
        test_sql_dialect("INSERT INTO IDENTIFIER('   ') SELECT * FROM s", "mssql")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["s"]),
            out_tables: tables(vec![])
        }
    );
}

#[test]
fn insert_into_identifier_quoted_parts_databricks_has_no_output() {
    // IDENTIFIER("db"."t1") passes identifier parts, not one string
    // literal; that form is not resolved, so no output is emitted
    // rather than a phantom one called IDENTIFIER - as on Snowflake.
    assert_eq!(
        test_sql_dialect(
            "INSERT INTO IDENTIFIER(\"db\".\"t1\") SELECT * FROM s",
            "databricks",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["s"]),
            out_tables: tables(vec![])
        }
    );
}

#[test]
fn select_from_identifier_four_part_name_keeps_last_three_parts() {
    // A four-part name keeps its last three components (database, schema,
    // table) - with IDENTIFIER exactly as without, since a plain
    // four-part name is treated the same way.
    for sql in [
        "SELECT * FROM IDENTIFIER('a.b.c.d')",
        "SELECT * FROM a.b.c.d",
    ] {
        assert_eq!(
            test_sql_dialect(sql, "snowflake").unwrap().table_lineage,
            TableLineage {
                in_tables: tables(vec!["b.c.d"]),
                out_tables: tables(vec![])
            }
        );
    }
}

#[test]
fn select_from_identifier_string_with_comment() {
    // The IDENTIFIER string is tokenized as SQL, so a comment inside it
    // is dropped: 't1 -- x' names the table t1.
    assert_eq!(
        test_sql_dialect("SELECT * FROM IDENTIFIER('t1 -- x')", "snowflake")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["t1"]),
            out_tables: tables(vec![])
        }
    );
}

#[test]
fn select_from_identifier_mssql_bracketed_parts() {
    // The string is parsed with the statement's dialect: under MsSql the
    // bracketed parts are three components, database "my db", schema
    // "dbo", table "t 1", each carrying its bracket quote style. (Under
    // the generic dialect the same string is a single name.)
    let output =
        test_sql_dialect("SELECT * FROM IDENTIFIER('[my db].[dbo].[t 1]')", "mssql").unwrap();
    assert_eq!(output.table_lineage.in_tables.len(), 1);
    let input = &output.table_lineage.in_tables[0];
    assert_eq!(input.database.as_deref(), Some("my db"));
    assert_eq!(input.schema.as_deref(), Some("dbo"));
    assert_eq!(input.name, "t 1");
    let quote_style = input.quote_style.as_ref().unwrap();
    assert_eq!(quote_style.database, Some('['));
    assert_eq!(quote_style.schema, Some('['));
    assert_eq!(quote_style.name, Some('['));
}

#[test]
fn select_from_identifier_name_with_space() {
    // A value that does not parse as a dotted name falls back to a single
    // table name, spaces included.
    assert_eq!(
        test_sql_dialect("SELECT * FROM IDENTIFIER('t1 t2')", "snowflake")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["t1 t2"]),
            out_tables: tables(vec![])
        }
    );
}

#[test]
fn select_from_identifier_unparseable_string_is_single_name() {
    // 'db.IDENTIFIER(''t'')' does not parse as identifiers only, so the
    // whole string becomes one table name.
    let output = test_sql_dialect(
        "SELECT * FROM IDENTIFIER('db.IDENTIFIER(''t'')')",
        "snowflake",
    )
    .unwrap();
    assert_eq!(output.table_lineage.in_tables.len(), 1);
    let input = &output.table_lineage.in_tables[0];
    assert_eq!(input.database, None);
    assert_eq!(input.schema, None);
    assert_eq!(input.name, "db.IDENTIFIER('t')");
}

#[test]
fn select_from_identifier_databricks_qualified() {
    assert_eq!(
        test_sql_dialect("SELECT * FROM IDENTIFIER('cat.sch.t')", "databricks")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["cat.sch.t"]),
            out_tables: tables(vec![])
        }
    );
}

#[test]
fn insert_all_when_second_subquery_table_not_hidden_by_first_subquery_cte() {
    assert_eq!(
        test_sql_dialect(
            "INSERT ALL WHEN x IN (WITH q AS (SELECT x FROM lk) SELECT x FROM q) \
             AND y IN (SELECT y FROM q) THEN INTO o1 SELECT x, y FROM src",
            "snowflake",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["lk", "q", "src"]),
            out_tables: tables(vec!["o1"])
        }
    );
}

#[test]
fn insert_source_with_referencing_statement_cte() {
    assert_eq!(
        test_sql(
            "WITH c AS (SELECT * FROM a) \
             INSERT INTO t1 WITH d AS (SELECT * FROM c) SELECT * FROM d"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["a"]),
            out_tables: tables(vec!["t1"])
        }
    );
}

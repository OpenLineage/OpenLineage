// Copyright 2018-2023 contributors to the OpenLineage project
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
            in_tables: tables(vec!["b1.b2", "m.dim"]),
            out_tables: tables(vec!["a1.a2"])
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
        test_sql(
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
    "
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
        test_sql(
            "
        INSERT OVERWRITE TABLE mytable
        PARTITION (ds = '2022-03-31')
        SELECT
            *
        FROM
        (SELECT * FROM table2) a"
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
        test_sql(
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
         "
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
            in_tables: tables(vec!["b1.b2"]),
            out_tables: tables(vec!["a1.a2"])
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
            in_tables: tables(vec!["b1.b2", "c1.c2", "d1.d2", "e1.e2", "f1.f2"]),
            out_tables: tables(vec!["a1.a2"])
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
        test_sql(
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
        "
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
        test_sql(
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
        "
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["ddw.aps2", "ddw.dps"]),
            out_tables: tables(vec!["ddw.aps2"])
        }
    )
}

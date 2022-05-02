use openlineage_sql::SqlMeta;

#[macro_use]
mod test_utils;
use test_utils::*;

#[test]
fn insert_values() {
    assert_eq!(
        test_sql("INSERT INTO TEST VALUES(1)",),
        SqlMeta {
            in_tables: vec![],
            out_tables: table("TEST")
        }
    );
}

#[test]
fn insert_cols_values() {
    assert_eq!(
        test_sql("INSERT INTO tbl(col1, col2) VALUES (1, 2), (2, 3)",),
        SqlMeta {
            in_tables: vec![],
            out_tables: table("tbl")
        }
    );
}

#[test]
fn insert_select_table() {
    assert_eq!(
        test_sql("INSERT INTO TEST SELECT * FROM TEMP",),
        SqlMeta {
            in_tables: table("TEMP"),
            out_tables: table("TEST")
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
        ),
        SqlMeta {
            in_tables: tables(vec!["b1.b2", "m.dim"]),
            out_tables: table("a1.a2")
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
    ), SqlMeta {
        in_tables: table("top_delivery_times"),
        out_tables: table("popular_orders_day_of_week")
    })
}

#[test]
fn insert_overwrite_table() {
    assert_eq!(
        test_sql(
            "\
        INSERT OVERWRITE TABLE schema.daily_play_sessions_v2
        PARTITION (ds = '2022-03-30')
        SELECT
            platform_id,
            universe_id,
            pii_userid,
            NULL as session_id,
            NULL as session_start_ts,
            COUNT(1) AS session_cnt,
            SUM(
                UNIX_TIMESTAMP(stopped) - UNIX_TIMESTAMP(joined)
            ) AS time_spent_sec
        FROM schema.fct_play_sessions_merged
        WHERE ds = '2022-03-30'
            AND UNIX_TIMESTAMP(stopped) - UNIX_TIMESTAMP(joined) BETWEEN 0 AND 28800
        GROUP BY
            platform_id,
            universe_id,
            pii_userid
    "
        ),
        SqlMeta {
            in_tables: table("schema.fct_play_sessions_merged"),
            out_tables: table("schema.daily_play_sessions_v2")
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
        ),
        SqlMeta {
            in_tables: table("table2"),
            out_tables: table("mytable")
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
        ),
        SqlMeta {
            in_tables: tables(vec!["table2", "table3", "table4"]),
            out_tables: table("mytable")
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
        ),
        SqlMeta {
            in_tables: table("b1.b2"),
            out_tables: table("a1.a2")
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
        ),
        SqlMeta {
            in_tables: tables(vec!["b1.b2", "c1.c2", "d1.d2", "e1.e2", "f1.f2"]),
            out_tables: table("a1.a2")
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
        ),
        SqlMeta {
            in_tables: vec![],
            out_tables: table("public.\"Employees\""),
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
        ),
        SqlMeta {
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
        ),
        SqlMeta {
            in_tables: table("a.a"),
            out_tables: tables(vec!["a.a", "b.b", "c.c"]),
        }
    )
}

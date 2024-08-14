// Copyright 2018-2024 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::*;

#[test]
fn table_reference_with_simple_ctes() {
    let query_string = "
        WITH tab1 AS (
            SELECT * FROM users as u
        ),
        tab2 AS (
            SELECT * FROM bots
        )
        SELECT r.id
        FROM python AS r
        UNION ALL
        SELECT id
        FROM tab2";

    let table_lineage = test_sql(query_string);

    assert_eq!(
        table_lineage
            .unwrap()
            .table_lineage
            .in_tables
            .iter()
            .map(|x| x.name.clone())
            .collect::<Vec<String>>(),
        vec!["bots", "python"]
    )
}

#[test]
fn table_reference_with_simple_q_ctes() {
    let query_string = "
        WITH tab1 AS (
            SELECT * FROM users
        )
        SELECT id
        FROM tab2";

    let table_lineage = test_sql(query_string);

    assert_eq!(
        table_lineage
            .unwrap()
            .table_lineage
            .in_tables
            .iter()
            .map(|x| x.name.clone())
            .collect::<Vec<String>>(),
        vec!["tab2"]
    )
}

#[test]
fn table_reference_with_passersby_ctes() {
    let query_string = "
        WITH tab1 AS (
            SELECT * FROM users as u
        ),
        tab2 AS (
            SELECT * FROM tab1
        ),
        tab3 AS (
            SELECT * FROM users2
        )
        SELECT id
        FROM tab2
        UNION ALL
        SELECT id
        FROM users3";

    let table_lineage = test_sql(query_string);

    assert_eq!(
        table_lineage
            .unwrap()
            .table_lineage
            .in_tables
            .iter()
            .map(|x| x.name.clone())
            .collect::<Vec<String>>(),
        vec!["users", "users3"]
    )
}

#[test]
fn table_references_with_simple_query() {
    let query_string = "
        SELECT * FROM tab1
        UNION ALL
        SELECT * FROM tab2";

    let table_lineage = test_sql(query_string);

    assert_eq!(
        table_lineage
            .unwrap()
            .table_lineage
            .in_tables
            .iter()
            .map(|x| x.name.clone())
            .collect::<Vec<String>>(),
        vec!["tab1", "tab2"]
    )
}

#[test]
fn table_references_with_subquery() {
    let query_string = "
        SELECT * FROM tab1
        UNION ALL
        SELECT * FROM (SELECT * FROM tab2)";

    let table_lineage = test_sql(query_string);

    assert_eq!(
        table_lineage
            .unwrap()
            .table_lineage
            .in_tables
            .iter()
            .map(|x| x.name.clone())
            .collect::<Vec<String>>(),
        vec!["tab1", "tab2"]
    )
}

#[test]
fn table_references_with_subquery_and_alias() {
    let query_string = "
        SELECT * FROM tab1
        UNION ALL
        SELECT * FROM (SELECT * FROM tab2) AS t";

    let table_lineage = test_sql(query_string);

    assert_eq!(
        table_lineage
            .unwrap()
            .table_lineage
            .in_tables
            .iter()
            .map(|x| x.name.clone())
            .collect::<Vec<String>>(),
        vec!["tab1", "tab2"]
    )
}

#[test]
fn table_references_with_subquery_and_alias_and_cte() {
    let query_string = "
        WITH tab1 AS (
            SELECT * FROM users as u
        )
        SELECT * FROM tab1
        UNION ALL
        SELECT * FROM (SELECT * FROM tab2) AS t";

    let table_lineage = test_sql(query_string);

    assert_eq!(
        table_lineage
            .unwrap()
            .table_lineage
            .in_tables
            .iter()
            .map(|x| x.name.clone())
            .collect::<Vec<String>>(),
        vec!["tab2", "users"]
    )
}

#[test]
fn table_references_complex_main_query_does_not_use_ctes() {
    let query_string = "
        WITH tab10 AS (
            SELECT * FROM users as u
        )
        SELECT * FROM animals1
        UNION ALL
        SELECT * FROM (SELECT * FROM animals2) AS t
        UNION ALL
        SELECT * FROM (SELECT * FROM animals3) AS t";

    let table_lineage = test_sql(query_string);

    assert_eq!(
        table_lineage
            .unwrap()
            .table_lineage
            .in_tables
            .iter()
            .map(|x| x.name.clone())
            .collect::<Vec<String>>(),
        vec!["animals1", "animals2", "animals3"]
    )
}

#[test]
fn table_references_complex_main_query_uses_ctes() {
    let query_string = "
        WITH tab10 AS (
            SELECT * FROM
            (SELECT * FROM users2 as u
            UNION
            SELECT * FROM users as u) as t
        )
        SELECT * FROM tab10
        UNION ALL
        SELECT * FROM (SELECT * FROM animals2) AS t
        UNION ALL
        SELECT * FROM (SELECT * FROM animals3) AS t";

    let table_lineage = test_sql(query_string);

    assert_eq!(
        table_lineage
            .unwrap()
            .table_lineage
            .in_tables
            .iter()
            .map(|x| x.name.clone())
            .collect::<Vec<String>>(),
        vec!["animals2", "animals3", "users", "users2"]
    )
}

#[test]
fn table_references_with_many_union_all() {
    let query_string = "
        WITH tab10 AS (
            SELECT * FROM
            (SELECT * FROM users2 as u
            UNION
            SELECT * FROM users as u
            UNION
            SELECT * FROM users3 as u
            UNION
            SELECT * FROM users4 as u
            ) as t
        )
        SELECT * FROM tab10
        UNION ALL
        SELECT * FROM (SELECT * FROM animals2) AS t
        UNION ALL
        SELECT * FROM (SELECT * FROM animals3) AS t";

    let table_lineage = test_sql(query_string);

    assert_eq!(
        table_lineage
            .unwrap()
            .table_lineage
            .in_tables
            .iter()
            .map(|x| x.name.clone())
            .collect::<Vec<String>>(),
        vec!["animals2", "animals3", "users", "users2", "users3", "users4"]
    )
}

#[test]
fn table_references_connected_ctes() {
    let query_string = "
        WITH tab1 AS (
            WITH data3 AS (
                WITH data8 AS (
                    SELECT r.* FROM (
                        SELECT * FROM users as u
                    ) as r
                ),
                data5 AS (
                    SELECT r.* FROM (
                        SELECT * FROM owners as u
                    ) as r
                ),
               data6 AS (
                    SELECT * FROM data5
                )
               SELECT * FROM data6
               UNION ALL
               SELECT * FROM data5
            ),
            data2 AS (
                SELECT * FROM data3
            )
            SELECT * FROM data2
        ),
        tab2 AS (
            SELECT * FROM tab1
        ),
        tab3 AS (
            SELECT * FROM tab2
        ),
        tab4 AS (
            SELECT * FROM users2
        ),
        tab5 AS (
            SELECT * FROM tab4
        )
        SELECT * FROM tab5";

    let table_lineage = test_sql(query_string);

    assert_eq!(
        table_lineage
            .unwrap()
            .table_lineage
            .in_tables
            .iter()
            .map(|x| x.name.clone())
            .collect::<Vec<String>>(),
        vec!["users2"]
    )
}

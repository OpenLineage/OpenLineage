// Copyright 2018-2025 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::*;
use openlineage_sql::{ColumnLineage, ColumnMeta};

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
fn table_complex() {
    let output_1 = test_sql(
        "
        SELECT COL1
        FROM
        (
            SELECT COL1
            FROM
                (
                    SELECT 
                    COL2
                        AS COL1
                    FROM TAB1
                  ) INNER_QUERY
        );",
    )
    .unwrap();

    let output_2 = test_sql(
        "
        SELECT COL1
        FROM
        (
            SELECT COL1
            FROM
                (
                    SELECT 
                    COL2
                        AS COL1
                    FROM TAB1
                    )
        );",
    )
    .unwrap();

    let output_3 = test_sql(
        "
        SELECT COL1
        FROM
        (
            SELECT COL1
            FROM
                (
                    SELECT 
                    COL2
                        AS COL1
                    FROM TAB1
                    ) INNER_QUERY
        ) OUTER_QUERY;",
    )
    .unwrap();

    // assert all outputs are the same
    assert_eq!(output_1, output_2);
    assert_eq!(output_2, output_3);

    assert_eq!(
        output_1.column_lineage,
        vec![ColumnLineage {
            descendant: ColumnMeta {
                origin: None,
                name: "COL1".to_string()
            },
            lineage: vec![ColumnMeta {
                origin: Some(table("TAB1")),
                name: "COL2".to_string()
            },]
        }]
    );
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

#[test]
fn test_complex_cte() {
    let output = test_sql(
        "with tab1 as (
                 SELECT s.col1, s.col2, s2.col3, s2.col4
                 from stg as s, stg2 AS s2
            ),
            tab2 as (
                 SELECT col1, col2, col3, col4 from tab1
            )
            SELECT col1, col2, col3, col4 FROM tab2",
    );

    assert_eq!(
        output.unwrap().column_lineage,
        vec![
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "col1".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("stg")),
                    name: "col1".to_string()
                },]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "col2".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("stg")),
                    name: "col2".to_string()
                },]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "col3".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("stg2")),
                    name: "col3".to_string()
                },]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "col4".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("stg2")),
                    name: "col4".to_string()
                },]
            }
        ]
    );
}

#[test]
fn test_column_resolving_in_complex_cte() {
    let output = test_sql(
        "with stage_1 as
            (
                   SELECT col_1, col_2, col_3, col_4 FROM source_tbl
                   WHERE date_time >= current_date AND  col_1 IN (SELECT distinct col_1 FROM source_tbl)
            ),
            stage_2 as
            (
                select col_1, col_2, col_3, col_4 from stage_1
            )
            select tl.col_1, s_acc.x,
            s_coa_acc.y,
            tl.col_3, tl.col_4
            from stage_2 tl
            join tbl2 s_acc on s_acc.x= tl.col_2
            left join tbl3 s_coa_acc on s_coa_acc.y= tl.col_2",
    )
        .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "col_1".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("source_tbl")),
                    name: "col_1".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "col_3".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("source_tbl")),
                    name: "col_3".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "col_4".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("source_tbl")),
                    name: "col_4".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "x".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("tbl2")),
                    name: "x".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "y".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("tbl3")),
                    name: "y".to_string()
                }]
            },
        ]
    );
}

#[test]
fn test_cte_with_cycles() {
    let output = test_sql(
        "WITH tab1 AS (
                SELECT s.col1, s.col2, s2.col3, s2.col4
                FROM stg AS s, stg2 AS s2
            ), tab1 AS (
                SELECT col1, col2, col3, col4
                FROM tab1
            )
            SELECT col1, col2, col3, col4
            FROM tab1",
    )
    .unwrap();

    assert_eq!(
        output.column_lineage,
        vec![
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "col1".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("stg")),
                    name: "col1".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "col2".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("stg")),
                    name: "col2".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "col3".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("stg2")),
                    name: "col3".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "col4".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("stg2")),
                    name: "col4".to_string()
                }]
            },
        ]
    );
}

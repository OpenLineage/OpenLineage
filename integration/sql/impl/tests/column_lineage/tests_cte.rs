// Copyright 2018-2024 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::{table, test_sql};
use openlineage_sql::{ColumnLineage, ColumnMeta};

#[test]
fn test_simple_cte() {
    let output = test_sql(
        "WITH my_cte AS (
            SELECT a,b,c FROM t1
         )
         SELECT a,c FROM my_cte",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "a".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("t1")),
                    name: "a".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "c".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("t1")),
                    name: "c".to_string()
                }]
            },
        ]
    );
}

#[test]
fn test_complex_cte() {
    let output = test_sql(
        "with stage_1 as
            (
                   SELECT col_1, col_2, col_3, col_4 FROM source_tbl
                   WHERE date_time >= current_date - (5 * interval '1 days')
                         AND  col_1
                                     IN (
                                             SELECT distinct col_1
                                             FROM source_tbl s_trans
                                             where <some-condition>
                                         )
                              )
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
                    name: "col_2".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("source_tbl")),
                    name: "col_2".to_string()
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

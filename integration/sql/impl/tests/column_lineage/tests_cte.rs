// Copyright 2018-2026 contributors to the OpenLineage project
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
fn test_cte_select_star() {
    let output = test_sql(
        "WITH data AS (SELECT x.a, x.b, x.c FROM tbl x)
         SELECT * FROM data",
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
                    origin: Some(table("tbl")),
                    name: "a".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "b".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("tbl")),
                    name: "b".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "c".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("tbl")),
                    name: "c".to_string()
                }]
            },
        ]
    );
}

#[test]
fn test_cte_qualified_wildcard() {
    let output = test_sql(
        "WITH data AS (SELECT a, b FROM tbl)
         SELECT data.* FROM data",
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
                    origin: Some(table("tbl")),
                    name: "a".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "b".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("tbl")),
                    name: "b".to_string()
                }]
            },
        ]
    );
}

#[test]
fn test_cte_wildcard_with_alias() {
    let output = test_sql(
        "WITH data AS (SELECT a, b FROM tbl)
         SELECT d.* FROM data AS d",
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
                    origin: Some(table("tbl")),
                    name: "a".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "b".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("tbl")),
                    name: "b".to_string()
                }]
            },
        ]
    );
}

#[test]
fn test_chained_cte_wildcard() {
    let output = test_sql(
        "WITH
           d1 AS (SELECT a, b FROM t1),
           d2 AS (SELECT * FROM d1)
         SELECT * FROM d2",
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
                    name: "b".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("t1")),
                    name: "b".to_string()
                }]
            },
        ]
    );
}

#[test]
fn test_multi_cte_join_wildcard() {
    let output = test_sql(
        "WITH
           d1 AS (SELECT a FROM t1),
           d2 AS (SELECT b FROM t2)
         SELECT * FROM d1 JOIN d2 ON d1.a = d2.b",
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
                    name: "b".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("t2")),
                    name: "b".to_string()
                }]
            },
        ]
    );
}

#[test]
fn test_cte_mixed_wildcard_and_explicit() {
    let output = test_sql(
        "WITH
           d1 AS (SELECT a FROM t1),
           d2 AS (SELECT b FROM t2)
         SELECT d1.*, d2.b FROM d1 JOIN d2 ON d1.a = d2.b",
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
                    name: "b".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("t2")),
                    name: "b".to_string()
                }]
            },
        ]
    );
}

#[test]
fn test_cte_inner_star_no_lineage() {
    let output = test_sql(
        "WITH data AS (SELECT * FROM physical_table)
         SELECT * FROM data",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![]);
}

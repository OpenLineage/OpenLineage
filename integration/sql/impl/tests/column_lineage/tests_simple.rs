// Copyright 2018-2022 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::test_sql;
use openlineage_sql::{ColumnLineage, ColumnMeta};

#[test]
fn test_simple_select() {
    let output = test_sql("SELECT a FROM table1 t1").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![ColumnLineage {
            descendant: ColumnMeta {
                name: "a".to_string()
            },
            lineage: vec![ColumnMeta {
                name: "t1.a".to_string()
            }]
        }]
    );
}

#[test]
fn test_simple_renaming() {
    let output = test_sql("SELECT t1.a, b, c as x, [d] as [y] FROM table1 t1").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            ColumnLineage {
                descendant: ColumnMeta {
                    name: "a".to_string()
                },
                lineage: vec![ColumnMeta {
                    name: "t1.a".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    name: "b".to_string()
                },
                lineage: vec![ColumnMeta {
                    name: "t1.b".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    name: "x".to_string()
                },
                lineage: vec![ColumnMeta {
                    name: "t1.c".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    name: "y".to_string()
                },
                lineage: vec![ColumnMeta {
                    name: "t1.d".to_string()
                }]
            },
        ]
    );
}

#[test]
fn test_simple_join() {
    let output = test_sql(
        "SELECT t1.a as x, t2.b as y
         FROM table1 t1
         INNER JOIN table2 t2
         ON t1.a = t2.a",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            ColumnLineage {
                descendant: ColumnMeta {
                    name: "x".to_string()
                },
                lineage: vec![ColumnMeta {
                    name: "t1.a".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    name: "y".to_string()
                },
                lineage: vec![ColumnMeta {
                    name: "t1.b".to_string()
                }]
            },
        ]
    );
}

#[test]
fn test_nested_select() {
    let output = test_sql(
        "SELECT t1.a
         FROM table1
         WHERE b IN (SELECT b FROM table2)",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![ColumnLineage {
            descendant: ColumnMeta {
                name: "a".to_string()
            },
            lineage: vec![ColumnMeta {
                name: "t1.a".to_string()
            }]
        },]
    );
}

#[test]
fn test_nested_renaming() {
    let output = test_sql(
        "SELECT t1.a, t2.c as d
         FROM table1 t1
         INNER JOIN (SELECT t2.a, t2.b as c FROM table2 t2) t2
         ON t1.a = t2.a",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            ColumnLineage {
                descendant: ColumnMeta {
                    name: "a".to_string()
                },
                lineage: vec![ColumnMeta {
                    name: "t1.a".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    name: "b".to_string()
                },
                lineage: vec![ColumnMeta {
                    name: "t1.b".to_string()
                }]
            },
        ]
    );
}

#[test]
fn test_simple_case() {
    let output = test_sql(
        "SELECT CASE WHEN a > b THEN c ELSE a END as d
         FROM table1",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![ColumnLineage {
            descendant: ColumnMeta {
                name: "d".to_string()
            },
            lineage: vec![
                ColumnMeta {
                    name: "t1.a".to_string()
                },
                ColumnMeta {
                    name: "t1.b".to_string()
                },
                ColumnMeta {
                    name: "t1.c".to_string()
                },
            ]
        },]
    );
}

#[test]
fn test_simple_operators() {
    let output = test_sql(
        "SELECT t1.a + b as c
         FROM table1 t1",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![ColumnLineage {
            descendant: ColumnMeta {
                name: "c".to_string()
            },
            lineage: vec![
                ColumnMeta {
                    name: "t1.a".to_string()
                },
                ColumnMeta {
                    name: "t1.b".to_string()
                },
            ]
        },]
    );
}

#[test]
fn test_simple_count() {
    let output = test_sql(
        "SELECT COUNT(t1.a) as b
         FROM table1 t1",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![ColumnLineage {
            descendant: ColumnMeta {
                name: "b".to_string()
            },
            lineage: vec![ColumnMeta {
                name: "t1.a".to_string()
            },]
        },]
    );
}

#[test]
fn test_simple_aggregate() {
    let output = test_sql(
        "SELECT RANK() OVER (PARTITION BY i.a ORDER BY i.b DESC) AS c
         FROM table1 i",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![ColumnLineage {
            descendant: ColumnMeta {
                name: "c".to_string()
            },
            lineage: vec![
                ColumnMeta {
                    name: "t1.a".to_string()
                },
                ColumnMeta {
                    name: "t1.b".to_string()
                },
            ]
        },]
    );
}

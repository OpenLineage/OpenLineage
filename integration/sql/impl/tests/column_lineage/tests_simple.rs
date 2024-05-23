// Copyright 2018-2024 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::{table, test_sql};
use openlineage_sql::{ColumnLineage, ColumnMeta};

#[test]
fn test_simple_select() {
    let output = test_sql("SELECT a FROM t1").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![ColumnLineage {
            descendant: ColumnMeta {
                origin: None,
                name: "a".to_string()
            },
            lineage: vec![ColumnMeta {
                origin: Some(table("t1")),
                name: "a".to_string()
            }]
        }]
    );
}

#[test]
fn test_simple_renaming() {
    let output = test_sql("SELECT t1.a, b, c as x, d as y FROM table1 t1").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "a".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("table1")),
                    name: "a".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "b".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("table1")),
                    name: "b".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "x".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("table1")),
                    name: "c".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "y".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("table1")),
                    name: "d".to_string()
                }]
            },
        ]
    );
}

#[test]
fn test_compound_names() {
    let output = test_sql("SELECT db.t.x as x, y, t1.z as z FROM db.t as t1").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "x".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("db.t")),
                    name: "x".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "y".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("db.t")),
                    name: "y".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "z".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("db.t")),
                    name: "z".to_string()
                }]
            },
        ]
    );
}

#[test]
fn test_column_name_inference() {
    let output = test_sql("SELECT db.t.a, b, (a + b) FROM db.t").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "_0".to_string()
                },
                lineage: vec![
                    ColumnMeta {
                        origin: Some(table("db.t")),
                        name: "a".to_string()
                    },
                    ColumnMeta {
                        origin: Some(table("db.t")),
                        name: "b".to_string()
                    },
                ]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "a".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("db.t")),
                    name: "a".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "b".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("db.t")),
                    name: "b".to_string()
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
                    origin: None,
                    name: "x".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("table1")),
                    name: "a".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "y".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("table2")),
                    name: "b".to_string()
                }]
            },
        ]
    );
}

#[test]
fn test_nested_select() {
    let output = test_sql(
        "SELECT t1.a
         FROM table1 t1
         WHERE b IN (SELECT b FROM table2)",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![ColumnLineage {
            descendant: ColumnMeta {
                origin: None,
                name: "a".to_string()
            },
            lineage: vec![ColumnMeta {
                origin: Some(table("table1")),
                name: "a".to_string()
            }]
        },]
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
                origin: None,
                name: "d".to_string()
            },
            lineage: vec![
                ColumnMeta {
                    origin: Some(table("table1")),
                    name: "a".to_string()
                },
                ColumnMeta {
                    origin: Some(table("table1")),
                    name: "b".to_string()
                },
                ColumnMeta {
                    origin: Some(table("table1")),
                    name: "c".to_string()
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
                origin: None,
                name: "c".to_string()
            },
            lineage: vec![
                ColumnMeta {
                    origin: Some(table("table1")),
                    name: "a".to_string()
                },
                ColumnMeta {
                    origin: Some(table("table1")),
                    name: "b".to_string()
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
                origin: None,
                name: "b".to_string()
            },
            lineage: vec![ColumnMeta {
                origin: Some(table("table1")),
                name: "a".to_string()
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
                origin: None,
                name: "c".to_string()
            },
            lineage: vec![
                ColumnMeta {
                    origin: Some(table("table1")),
                    name: "a".to_string()
                },
                ColumnMeta {
                    origin: Some(table("table1")),
                    name: "b".to_string()
                },
            ]
        },]
    );
}

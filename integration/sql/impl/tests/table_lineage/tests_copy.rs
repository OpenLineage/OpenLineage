// Copyright 2018-2024 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::tables;
use openlineage_sql::{parse_sql, DbTableMeta, TableLineage};
use sqlparser::dialect::SnowflakeDialect;

#[test]
fn parse_copy_into_table() {
    let meta = parse_sql(
        "COPY INTO SCHEMA.SOME_MONITORING_SYSTEM
        FROM (SELECT t.$1:st AS st FROM @schema.general_finished)",
        &SnowflakeDialect {},
        None,
    )
    .unwrap();
    assert_eq!(
        meta.table_lineage,
        TableLineage {
            in_tables: vec![DbTableMeta::new_default_dialect_with_namespace_and_schema(
                "@schema.general_finished".to_string(),
                true,
                true,
            )],
            out_tables: tables(vec!["SCHEMA.SOME_MONITORING_SYSTEM"])
        }
    )
}

#[test]
fn parse_copy_into_with_snowflake_stage_locations() {
    // gcs with single quote
    assert_eq!(
        parse_sql(
            "COPY INTO my_company.emp_basic FROM 'gcs://mybucket/./../a.csv'",
            &SnowflakeDialect {},
            None,
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: vec![DbTableMeta::new_default_dialect_with_namespace_and_schema(
                "gcs://mybucket/./../a.csv".to_string(),
                true,
                true,
            )],
            out_tables: tables(vec!["my_company.emp_basic"])
        }
    );

    // s3 with double quote
    assert_eq!(
        parse_sql(
            "COPY INTO my_company.emp_basic FROM \"s3://mybucket/./../a.csv\"",
            &SnowflakeDialect {},
            None,
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: vec![DbTableMeta::new_default_dialect_with_namespace_and_schema(
                "s3://mybucket/./../a.csv".to_string(),
                true,
                true,
            )],
            out_tables: tables(vec!["my_company.emp_basic"])
        }
    );

    // azure location without quotes
    assert_eq!(
        parse_sql(
            "COPY INTO my_company.emp_basic FROM 'azure://mybucket/./../a.csv'",
            &SnowflakeDialect {},
            None,
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: vec![DbTableMeta::new_default_dialect_with_namespace_and_schema(
                "azure://mybucket/./../a.csv".to_string(),
                true,
                true,
            )],
            out_tables: tables(vec!["my_company.emp_basic"])
        }
    );
}

#[test]
fn parse_copy_into_with_snowflake_internal_stages() {
    let stage_names = vec![
        "@namespace.stage_name",
        "@namespace.stage_name/path",
        "@namespace.%table_name/path",
        "@~/path",
    ];

    for stage_name in stage_names {
        assert_eq!(
            parse_sql(
                format!("COPY INTO a.b FROM {}", stage_name).as_str(),
                &SnowflakeDialect {},
                None,
            )
            .unwrap()
            .table_lineage,
            TableLineage {
                out_tables: tables(vec!["a.b"]),
                in_tables: vec![DbTableMeta::new_default_dialect_with_namespace_and_schema(
                    stage_name.to_string(),
                    true,
                    true,
                )],
            }
        );
    }
}

#[test]
fn parse_pivot_table() {
    assert_eq!(
        parse_sql(
            concat!(
                "SELECT * FROM monthly_sales AS a ",
                "PIVOT(SUM(a.amount) FOR a.MONTH IN ('JAN', 'FEB', 'MAR', 'APR')) AS p (c, d) ",
                "ORDER BY EMPID"
            ),
            &SnowflakeDialect {},
            None,
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["monthly_sales"]),
            out_tables: tables(vec![])
        }
    );
}

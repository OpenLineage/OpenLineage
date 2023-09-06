// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::*;
use openlineage_sql::{parse_sql, DbTableMeta, TableLineage};
use sqlparser::dialect::SnowflakeDialect;

#[test]
fn test_create_table() {
    assert_eq!(
        test_sql(
            "
        CREATE TABLE Persons (
        PersonID int,
        LastName varchar(255),
        FirstName varchar(255),
        Address varchar(255),
        City varchar(255));"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: vec![],
            out_tables: tables(vec!["Persons"])
        }
    )
}

#[test]
fn test_create_table_like() {
    assert_eq!(
        test_sql("CREATE TABLE new LIKE original")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["original"]),
            out_tables: tables(vec!["new"])
        }
    )
}

#[test]
fn test_create_table_clone() {
    assert_eq!(
        test_sql("CREATE OR REPLACE TABLE new CLONE original")
            .unwrap()
            .table_lineage,
        TableLineage {
            in_tables: tables(vec!["original"]),
            out_tables: tables(vec!["new"])
        }
    )
}

#[test]
fn test_create_and_insert() {
    assert_eq!(
        test_sql(
            "
        CREATE TABLE Persons (
        key int,
        value varchar(255));
        INSERT INTO Persons SELECT key, value FROM temp.table;"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["temp.table"]),
            out_tables: tables(vec!["Persons"])
        }
    )
}

#[test]
fn create_and_insert_multiple_stmts() {
    assert_eq!(
        test_multiple_sql(vec![
            "CREATE TABLE Persons (key int, value varchar(255));",
            "INSERT INTO Persons SELECT key, value FROM temp.table;"
        ])
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["temp.table"]),
            out_tables: tables(vec!["Persons"])
        }
    )
}

#[test]
fn create_hive_external_table_if_not_exist() {
    assert_eq!(
        test_sql_dialect(
            "
            CREATE EXTERNAL TABLE IF NOT EXISTS  Testing_Versions_latest (
                `ID` INT,
                `ExperimentID` INT,
                `Version` SMALLINT,
                `EnrollmentPeriodInHours` INT,
                `Started` TIMESTAMP,
                `EndDate` TIMESTAMP,
                `IsActive` BOOLEAN,
                `PercentOfSubjectsToEnroll` SMALLINT,
                `Created` TIMESTAMP,
                `Updated` TIMESTAMP,
                        ds STRING
                    )
                    STORED AS PARQUET
                    LOCATION 's3://abc.ingest/sqlserver/Testing/Versions/ds=2022-08-10'
                    TBLPROPERTIES ('parquet.compression'='SNAPPY');
        ",
            "hive"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: vec![],
            out_tables: tables(vec!["Testing_Versions_latest"])
        }
    )
}

#[test]
fn create_replace_as_select() {
    assert_eq!(
        test_sql(
            "
            CREATE OR REPLACE TABLE DATA_TEAM_DEMOS.ALL_DAYS AS (
            SELECT date AS calendar_day, yyyy_mm as calendar_month, yyyy as calendar_year
            FROM dwh_dev.commons.calendar
            WHERE date BETWEEN '2022-01-01' AND CURRENT_DATE
        )"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["dwh_dev.commons.calendar"]),
            out_tables: tables(vec!["DATA_TEAM_DEMOS.ALL_DAYS"])
        }
    )
}

#[test]
fn create_view() {
    assert_eq!(
        test_sql(
            "
            CREATE OR REPLACE VIEW DATA_TEAM_DEMOS.ALL_DAYS AS (
            SELECT date AS calendar_day, yyyy_mm as calendar_month, yyyy as calendar_year
            FROM dwh_dev.commons.calendar
            WHERE date BETWEEN '2022-01-01' AND CURRENT_DATE
        )"
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["dwh_dev.commons.calendar"]),
            out_tables: tables(vec!["DATA_TEAM_DEMOS.ALL_DAYS"])
        }
    )
}

#[test]
fn create_stage() {
    assert_eq!(
        parse_sql(
            "
             CREATE STAGE IF NOT EXISTS IN_OPENLINEAGE.DATA
             URL='gcs://openlineage/data/'
             storage_integration = GCS_INT
             FILE_FORMAT=(TYPE=PARQUET TRIM_SPACE=TRUE)
             COPY_OPTIONS=(MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE)
         ",
            &SnowflakeDialect {},
            None,
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: vec![DbTableMeta::new_default_dialect_with_namespace_and_schema(
                "gcs://openlineage/data/".to_string(),
                true,
                true,
            )],
            out_tables: vec![DbTableMeta::new_default_dialect_with_namespace_and_schema(
                "IN_OPENLINEAGE.DATA".to_string(),
                false,
                true
            )]
        }
    )
}

// Copyright 2018-2022 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use sqlparser::dialect::HiveDialect;
use openlineage_sql::SqlMeta;

#[macro_use]
mod test_utils;
use test_utils::*;

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
        ),
        SqlMeta {
            in_tables: vec![],
            out_tables: table("Persons")
        }
    )
}

#[test]
fn test_create_table_like() {
    assert_eq!(
        test_sql("CREATE TABLE new LIKE original"),
        SqlMeta {
            in_tables: table("original"),
            out_tables: table("new")
        }
    )
}

#[test]
fn test_create_table_clone() {
    assert_eq!(
        test_sql("CREATE OR REPLACE TABLE new CLONE original"),
        SqlMeta {
            in_tables: table("original"),
            out_tables: table("new")
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
        ),
        SqlMeta {
            in_tables: tables(vec!["temp.table"]),
            out_tables: table("Persons")
        }
    )
}

#[test]
fn create_and_insert_multiple_stmts() {
    assert_eq!(
        test_multiple_sql(vec![
            "CREATE TABLE Persons (key int, value varchar(255));",
            "INSERT INTO Persons SELECT key, value FROM temp.table;"
        ]),
        SqlMeta {
            in_tables: tables(vec!["temp.table"]),
            out_tables: table("Persons")
        }
    )
}

#[test]
fn create_hive_external_table_if_not_exist() {
    assert_eq!(
        test_sql_dialect("
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
        ", "hive"
        ), SqlMeta {
            in_tables: vec![],
            out_tables: table("Testing_Versions_latest")
        }
    )
}

#[test]
fn create_replace_as_select() {
    assert_eq!(
        test_sql("
            CREATE OR REPLACE TABLE DATA_TEAM_DEMOS.ALL_DAYS AS (
            SELECT date AS calendar_day, yyyy_mm as calendar_month, yyyy as calendar_year
            FROM dwh_dev.commons.calendar
            WHERE date BETWEEN '2022-01-01' AND CURRENT_DATE
        )"),
        SqlMeta {
            in_tables: table("dwh_dev.commons.calendar"),
            out_tables: table("DATA_TEAM_DEMOS.ALL_DAYS")
        }
    )
}
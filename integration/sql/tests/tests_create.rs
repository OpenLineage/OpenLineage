use openlineage_sql::{parse_sql, SqlMeta};
use sqlparser::dialect::PostgreSqlDialect;

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

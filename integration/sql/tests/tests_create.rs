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

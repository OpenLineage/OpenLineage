use openlineage_sql::{get_dialect, get_generic_dialect, parse_sql, parse_multiple_statements, DbTableMeta, SqlMeta};
use sqlparser::dialect::PostgreSqlDialect;

pub fn test_sql(sql: &str) -> SqlMeta {
    test_sql_dialect(sql, "postgres")
}

pub fn test_multiple_sql(sqls: Vec<&str>) -> SqlMeta {
    test_multiple_sql_dialect(sqls, "postgres")
}

pub fn test_multiple_sql_dialect(sqls: Vec<&str>, dialect: &str) -> SqlMeta {
    match parse_multiple_statements(sqls, get_dialect(dialect), None) {
        Ok(meta) => meta,
        Err(err) => {
            println!("{}", err);
            panic!("")
        }
    }
}


pub fn test_sql_dialect(sql: &str, dialect: &str) -> SqlMeta {
    match parse_sql(sql, get_dialect(dialect), None) {
        Ok(meta) => meta,
        Err(err) => {
            println!("{}", err);
            panic!("")
        }
    }
}

pub fn table(name: &str) -> Vec<DbTableMeta> {
    vec![DbTableMeta::py_new(String::from(name))]
}

pub fn tables(names: Vec<&str>) -> Vec<DbTableMeta> {
    names
        .into_iter()
        .map(|name| DbTableMeta::py_new(String::from(name)))
        .collect()
}

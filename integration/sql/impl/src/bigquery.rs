// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use sqlparser::dialect::Dialect;

#[derive(Debug, Default)]
pub struct BigQueryDialect;

impl Dialect for BigQueryDialect {
    // see https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"' || ch == '`'
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        ('a'..='z').contains(&ch) || ('A'..='Z').contains(&ch) || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.is_identifier_start(ch)
            || ('0'..='9').contains(&ch)
            || ch == '$'
            || ch == '-'
            || ch == '`'
    }
}

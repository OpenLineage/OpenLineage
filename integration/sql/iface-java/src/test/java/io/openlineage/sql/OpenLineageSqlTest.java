/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

class OpenLineageSqlTest {
    @Test
    void basicParse() {
        SqlMeta output = OpenLineageSql.parse(Arrays.asList("SELECT * FROM test")).get();
        assertEquals(output, new SqlMeta(
            Arrays.asList(new DbTableMeta(null, null, "test")),
            new ArrayList<DbTableMeta>(),
            Collections.emptyList()
        ));
    }

    @Test
    void parseWithDialect() {
        SqlMeta output = OpenLineageSql.parse(Arrays.asList("SELECT * FROM `random-project`.`dbt_test1`.`source_table` WHERE id = 1"), "bigquery").get();
        assertEquals(output, new SqlMeta(
            Arrays.asList(new DbTableMeta("random-project", "dbt_test1", "source_table")),
            new ArrayList<DbTableMeta>(),
            Collections.emptyList()
        ));
    }

    @Test
    void parseError() {
        boolean exceptionCaught = false;
        try {
            SqlMeta output = OpenLineageSql.parse(Arrays.asList("Definitely not an SQL statement")).get();
        } catch (Exception e) {
            exceptionCaught = true;
            assertTrue(e instanceof RuntimeException);
        }
        assertTrue(exceptionCaught);
    }
}

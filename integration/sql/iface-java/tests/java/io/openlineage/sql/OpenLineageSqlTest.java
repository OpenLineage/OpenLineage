package io.openlineage.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;

class OpenLineageSqlTest {
    @Test
    void basicParse() {
        SqlMeta output = OpenLineageSql.parse(Arrays.asList("SELECT * FROM test"));
        assertEquals(output, new SqlMeta(
            Arrays.asList(new DbTableMeta(null, null, "test")),
            new ArrayList<DbTableMeta>()
        ));
    }

    @Test
    void parseWithDialect() {
        SqlMeta output = OpenLineageSql.parse(Arrays.asList("SELECT * FROM `random-project`.`dbt_test1`.`source_table` WHERE id = 1"), "bigquery");
        assertEquals(output, new SqlMeta(
            Arrays.asList(new DbTableMeta("random-project", "dbt_test1", "source_table")),
            new ArrayList<DbTableMeta>()
        ));
    }

    @Test
    void parseError() {
        boolean exceptionCaught = false;
        try {
            SqlMeta output = OpenLineageSql.parse(Arrays.asList("Definitely not an SQL statement"));
        } catch (Exception e) {
            exceptionCaught = true;
            assertTrue(e instanceof RuntimeException);
        }
        assertTrue(exceptionCaught);
    }
}

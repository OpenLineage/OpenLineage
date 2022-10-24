/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

import io.openlineage.sql.OpenLineageSql;
import io.openlineage.sql.SqlMeta;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;

public class TestParser {
    public static void main(String[] args) throws Exception {
        if (args.length > 0) {
            SqlMeta output = OpenLineageSql.parse(Arrays.asList(args)).get();
            System.out.println(output);
            return;
        }

        // If not command line args were provided, we run an infinite loop
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            String sql = reader.readLine();
            SqlMeta output = OpenLineageSql.parse(Arrays.asList(sql)).get();
            System.out.println(output);
        }
    }
}

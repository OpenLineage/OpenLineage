/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import org.apache.spark.package$;

import java.util.HashMap;
import java.util.Map;

public class DatasetBuilderFactoryProvider {

    private static final Map<String, String> builderVersions = new HashMap<String, String>() {{
        put("2.4", "io.openlineage.spark.agent.lifecycle.Spark2DatasetBuilderFactory");
        put("3.1", "io.openlineage.spark.agent.lifecycle.Spark3DatasetBuilderFactory");
        put("3.2", "io.openlineage.spark.agent.lifecycle.Spark32DatasetBuilderFactory");
    }};

    static DatasetBuilderFactory getInstance() {
        String version = package$.MODULE$.SPARK_VERSION();
        try {
            return (DatasetBuilderFactory) Class.forName(builderVersions.get(version.substring(0, 3))).newInstance();
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Can't instantiate dataset builder factory factory for version: %s", version),
                    e);
        }
    }
}

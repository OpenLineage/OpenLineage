/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.facets.RuntimePropertyFacet;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RuntimePropertyFacetBuilderTest {
    @Test
    void testBuildDefault() {
        SparkSession session = mock(SparkSession.class);
        RuntimeConfig runtimeConfig = mock(RuntimeConfig.class);
        when(runtimeConfig.get("spark.openlineage.capturedRuntimeProperties")).thenReturn("mock-key1,mock-key2");
        when(runtimeConfig.get("mock-key1")).thenReturn("mock-value1");
        when(runtimeConfig.get("mock-key2")).thenReturn("mock-value2");
        when(session.conf()).thenReturn(runtimeConfig);
        SparkSession.setActiveSession(session);
        RuntimePropertyFacetBuilder builder = new RuntimePropertyFacetBuilder();
        Map<String, OpenLineage.RunFacet> runFacetMap = new HashMap<>();
        builder.build(new SparkListenerSQLExecutionEnd(1, 1L), runFacetMap::put);
        assertThat(runFacetMap)
                .hasEntrySatisfying(
                        "spark_properties",
                        facet -> {
                            assertThat(facet).isInstanceOf(RuntimePropertyFacet.class);
                            assertThat(((RuntimePropertyFacet) facet).getRuntimeProperties())
                                    .containsEntry("mock-key1", "mock-value1")
                                    .containsEntry("mock-key2", "mock-value2");
                        });

    }
}

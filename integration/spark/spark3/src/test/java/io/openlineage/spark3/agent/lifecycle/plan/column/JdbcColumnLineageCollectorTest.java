/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.sql.ColumnMeta;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JdbcColumnLineageCollectorTest {
    ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
    ExprId exprId = mock(ExprId.class);
    JDBCRelation relation = mock(JDBCRelation.class);
    String jdbcQuery =
            "(select js1.k, CONCAT(js1.j1, js2.j2) as j from jdbc_source1 js1 join jdbc_source2 js2 on js1.k = js2.k) SPARK_GEN_SUBQ_0";
    @BeforeEach
    void setup() {
        when(relation.jdbcOptions().tableOrQuery()).thenReturn(jdbcQuery);
        when(builder.getMapping(any(ColumnMeta.class))).thenReturn(exprId);
    }

    @Test
    void testInputCollection(){
        JdbcColumnLineageCollector.extractExternalInputs(relation, builder, Collections.emptyList());
    }
}

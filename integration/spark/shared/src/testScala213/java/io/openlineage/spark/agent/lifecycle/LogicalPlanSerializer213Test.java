/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.io.IOException;
import lombok.SneakyThrows;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.Seq;

class LogicalPlanSerializer213Test {
  private final LogicalPlanSerializer logicalPlanSerializer = new LogicalPlanSerializer();

  @Test
  void testSerializeLogicalPlanReturnsAlwaysValidJson() {
    LogicalPlan notSerializablePlan =
        new LogicalPlan() {
          @Override
          public Seq<Attribute> output() {
            return null;
          }

          @Override
          public Seq<LogicalPlan> children() {
            return null;
          }

          @Override
          public LogicalPlan withNewChildrenInternal(IndexedSeq<LogicalPlan> newChildren) {
            return null;
          }

          @Override
          public Object productElement(int n) {
            return null;
          }

          @Override
          public int productArity() {
            return 0;
          }

          @Override
          public boolean canEqual(Object that) {
            return false;
          }
        };
    final ObjectMapper mapper = new ObjectMapper();
    try {
      Logger.getLogger(logicalPlanSerializer.getClass()).setLevel(Level.ERROR);
      mapper.readTree(logicalPlanSerializer.serialize(notSerializablePlan));
    } catch (IOException e) {
      fail();
    }
  }

  @Test
  @SneakyThrows
  void testSerializeFiltersFields() {
    LogicalPlan plan =
        new LogicalPlan() {
          public SessionCatalog sessionCatalog =
              new SessionCatalog(mock(ExternalCatalog.class)) {
                public FunctionRegistry functionRegistry = mock(FunctionRegistry.class);
              };

          @Override
          public Seq<Attribute> output() {
            return null;
          }

          @Override
          public StructType schema() {
            return new StructType(
                new StructField[] {
                  new StructField(
                      "aString",
                      StringType$.MODULE$,
                      false,
                      new Metadata(new HashMap<String, Object>()))
                });
          }

          @Override
          public Seq<LogicalPlan> children() {
            return ScalaConversionUtils.<LogicalPlan>asScalaSeqEmpty();
          }

          @Override
          public LogicalPlan withNewChildrenInternal(IndexedSeq<LogicalPlan> newChildren) {
            return null;
          }

          public LogicalPlan withNewChildrenInternal(
              scala.collection.IndexedSeq<LogicalPlan> newChildren) {
            return null;
          }

          @Override
          public Object productElement(int n) {
            return null;
          }

          @Override
          public int productArity() {
            return 0;
          }

          @Override
          public boolean canEqual(Object that) {
            return false;
          }
        };

    assertFalse(logicalPlanSerializer.serialize(plan).contains("functionRegistry"));
    assertFalse(
        logicalPlanSerializer.serialize(plan).contains("Unable to serialize logical plan due to"));
  }

  @Test
  void testSerializeSlicesExcessivePayload() {
    LogicalPlan plan =
        new LogicalPlan() {

          public String okField = "some-field}}}}{{{\"";
          public String longField = RandomStringUtils.random(100000);

          @Override
          public Seq<Attribute> output() {
            return null;
          }

          @Override
          public StructType schema() {
            return null;
          }

          @Override
          public Seq<LogicalPlan> children() {
            return ScalaConversionUtils.<LogicalPlan>asScalaSeqEmpty();
          }

          @Override
          public LogicalPlan withNewChildrenInternal(IndexedSeq<LogicalPlan> newChildren) {
            return null;
          }

          public LogicalPlan withNewChildrenInternal(
              scala.collection.IndexedSeq<LogicalPlan> newChildren) {
            return null;
          }

          @Override
          public Object productElement(int n) {
            return null;
          }

          @Override
          public int productArity() {
            return 0;
          }

          @Override
          public boolean canEqual(Object that) {
            return false;
          }
        };

    String serializedPlanString = logicalPlanSerializer.serialize(plan);

    assertTrue(serializedPlanString.length() < 51000); // few extra bytes for json encoding
    assertTrue(serializedPlanString.contains("some-field}}}}{{{\\\\\\\""));
    try {
      new ObjectMapper().readTree(serializedPlanString);
    } catch (IOException e) {
      fail(); // not a valid JSON
    }
  }
}

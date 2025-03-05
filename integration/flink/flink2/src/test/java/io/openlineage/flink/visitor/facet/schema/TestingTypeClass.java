/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet.schema;

import java.util.List;

/** Testing class for GenericTypeInfoSchemaBuilder. */
public class TestingTypeClass {

  public String fieldA;

  @SuppressWarnings("PMD.UnusedPrivateField")
  private String fieldB; // shouldn't be present in schema facet

  public Long fieldC;

  public Long[] fieldD;

  public List<String> fieldE;

  public TestingSubTypeClass subType;
}

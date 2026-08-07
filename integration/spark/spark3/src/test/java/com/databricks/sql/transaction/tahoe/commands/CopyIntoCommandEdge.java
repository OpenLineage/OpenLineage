/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package com.databricks.sql.transaction.tahoe.commands;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Stands in for the Databricks {@code CopyIntoCommandEdge} that a {@code COPY INTO} statement is
 * resolved to on Spark 4.0 runtimes. The real class is not on any compile classpath, so extraction
 * has to work by reflection and cannot assume the member names.
 *
 * <p>The members are deliberately named nothing like {@code target} or {@code sourcePath}, and the
 * format name is declared before the location, so a test using this class only passes when
 * extraction is genuinely name-independent.
 *
 * <p>This is not a {@link LogicalPlan}: Java cannot extend the Scala plan classes because of the
 * covariant {@code withNewChildrenInternal} override. The reflective extraction works on plain
 * objects, so the fixture exercises the same code the runtime hits.
 */
public class CopyIntoCommandEdge {

  private final String fileFormatName;
  private final LogicalPlan copyIntoTargetRelation;
  private final LogicalPlan copyIntoSourceRelation;
  private final String ingestLocationUri;

  public CopyIntoCommandEdge(
      String fileFormatName,
      LogicalPlan copyIntoTargetRelation,
      LogicalPlan copyIntoSourceRelation,
      String ingestLocationUri) {
    this.fileFormatName = fileFormatName;
    this.copyIntoTargetRelation = copyIntoTargetRelation;
    this.copyIntoSourceRelation = copyIntoSourceRelation;
    this.ingestLocationUri = ingestLocationUri;
  }
}

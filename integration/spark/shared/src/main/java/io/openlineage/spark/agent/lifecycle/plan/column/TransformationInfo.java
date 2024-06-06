/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
public class TransformationInfo {

  @Getter @Setter private String type;

  @Getter @Setter private String subType;
  @Getter @Setter private String description;
  @Getter @Setter private Boolean masking;

  public static TransformationInfo identity() {
    return new TransformationInfo("IDENTITY", "IDENTITY", "", false);
  }

  public static TransformationInfo transformation() {
    return new TransformationInfo("TRANSFORMED", "TRANSFORMATION", "", false);
  }

  public static TransformationInfo aggregation() {
    return new TransformationInfo("TRANSFORMED", "AGGREGATION", "", false);
  }

  public static TransformationInfo indirect(String subType) {
    return new TransformationInfo("INDIRECT", subType, "", false);
  }
  // easiest way to compare two transformations
  public Integer numValue() { // FIXME - THAT'S SOOO UGLY, NEED TO FIX LATER
    if ("INDIRECT".equals(type)) {
      return 1;
    } else if ("TRANSFORMED".equals(type)) {
      if ("AGGREGATION".equals(subType)) {
        return 2;
      }
      return 3;
    }
    return 4;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TransformationInfo that = (TransformationInfo) o;
    return Objects.equals(type, that.type)
        && Objects.equals(subType, that.subType)
        && Objects.equals(description, that.description)
        && Objects.equals(masking, that.masking);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, subType, description, masking);
  }

  public TransformationInfo merge(TransformationInfo t) {
    if (t != null) {
      TransformationInfo res = this.numValue() < t.numValue() ? this : t;
      return new TransformationInfo(
          res.getType(),
          res.getSubType(),
          res.getDescription(),
          this.getMasking() || t.getMasking());
    }
    return null;
  }
}

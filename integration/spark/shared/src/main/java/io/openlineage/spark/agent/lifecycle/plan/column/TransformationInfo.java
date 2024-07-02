/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import io.openlineage.client.OpenLineage;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
public class TransformationInfo {

  public enum Types {
    INDIRECT,
    DIRECT
  }

  public enum Subtypes {
    AGGREGATION,
    TRANSFORMATION,
    IDENTITY,
    CONDITIONAL,
    SORT,
    GROUP_BY,
    JOIN,
    FILTER
  }

  @Getter @Setter private Types type;
  @Getter @Setter private Subtypes subType;
  @Getter @Setter private String description;
  @Getter @Setter private Boolean masking;

  public static TransformationInfo identity() {
    return new TransformationInfo(Types.DIRECT, Subtypes.IDENTITY, "", false);
  }

  public static TransformationInfo transformation() {
    return new TransformationInfo(Types.DIRECT, Subtypes.TRANSFORMATION, "", false);
  }

  public static TransformationInfo aggregation() {
    return new TransformationInfo(Types.DIRECT, Subtypes.AGGREGATION, "", false);
  }

  public static TransformationInfo indirect(Subtypes subType) {
    return new TransformationInfo(Types.INDIRECT, subType, "", false);
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
    TransformationInfo res;
    if (this.type.equals(Types.INDIRECT)) {
      res = this;
    } else if (t != null) {
      if (t.type.ordinal() < this.type.ordinal()) {
        res = t;
      } else if (t.subType.ordinal() < this.subType.ordinal()) {
        res = t;
      } else {
        res = this;
      }
    } else {
      return null;
    }
    return new TransformationInfo(
        res.getType(), res.getSubType(), res.getDescription(), this.getMasking() || t.getMasking());
  }

  public OpenLineage.ColumnLineageDatasetFacetFieldsAdditionalInputFieldsTransformations
      toInputFieldsTransformations() {
    return new OpenLineage
            .ColumnLineageDatasetFacetFieldsAdditionalInputFieldsTransformationsBuilder()
        .type(type.name())
        .subtype(subType.name())
        .description(description)
        .masking(masking)
        .build();
  }

  @Override
  public String toString() {
    return "TransformationInfo("
        + type
        + ", "
        + subType
        + ", '"
        + description
        + '\''
        + ", "
        + masking
        + ')';
  }
}

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

/**
 * Stores information about transformation type of the dependency between two expressions. The
 * transformation is described by 3 attributes: type, subtype and masking. {@link
 * TransformationInfo#type} indicate whether the transformation is direct (target value was derived
 * from source value) or indirect (target value was influenced by source value). {@link
 * TransformationInfo#subType} further divide the transformations. {@link
 * TransformationInfo#masking} indicates whether the transformation obfuscated the source value
 */
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
  /**
   * Method that simplifies the creation of an {@link TransformationInfo} object representing {@link
   * Types#DIRECT}, {@link Subtypes#IDENTITY} transformation.
   */
  public static TransformationInfo identity() {
    return new TransformationInfo(Types.DIRECT, Subtypes.IDENTITY, "", false);
  }
  /**
   * Method that simplifies the creation of an {@link TransformationInfo} object representing
   * non-masking, {@link Types#DIRECT}, {@link Subtypes#TRANSFORMATION} transformation.
   */
  public static TransformationInfo transformation() {
    return transformation(false);
  }
  /**
   * Method that simplifies the creation of {@link TransformationInfo} object representing {@link
   * Types#DIRECT}, {@link Subtypes#TRANSFORMATION} transformation.
   *
   * @param isMasking - is transformation masking e.g. {@link
   *     org.apache.spark.sql.catalyst.expressions.Sha1}
   */
  public static TransformationInfo transformation(Boolean isMasking) {
    return new TransformationInfo(Types.DIRECT, Subtypes.TRANSFORMATION, "", isMasking);
  }
  /**
   * Method that simplifies the creation of an {@link TransformationInfo} object representing
   * non-masking, {@link Types#DIRECT}, {@link Subtypes#AGGREGATION} transformation.
   */
  public static TransformationInfo aggregation() {
    return aggregation(false);
  }
  /**
   * Method that simplifies the creation of {@link TransformationInfo} object representing {@link
   * Types#DIRECT}, {@link Subtypes#AGGREGATION} transformation.
   *
   * @param isMasking - is transformation masking e.g. {@link
   *     org.apache.spark.sql.catalyst.expressions.aggregate.Count}
   */
  public static TransformationInfo aggregation(Boolean isMasking) {
    return new TransformationInfo(Types.DIRECT, Subtypes.AGGREGATION, "", isMasking);
  }
  /**
   * Method that simplifies the creation of {@link TransformationInfo} object representing
   * non-masking, {@link Types#INDIRECT} transformation.
   *
   * @param subType - the subtype of the transformation viable subtypes: {@link
   *     Subtypes#CONDITIONAL},{@link Subtypes#SORT},{@link Subtypes#GROUP_BY},{@link
   *     Subtypes#JOIN},{@link Subtypes#FILTER},
   */
  public static TransformationInfo indirect(Subtypes subType) {
    return TransformationInfo.indirect(subType, false);
  }
  /**
   * Method that simplifies the creation of {@link TransformationInfo} object representing {@link
   * Types#INDIRECT} transformation.
   *
   * @param subType - the subtype of the transformation viable subtypes: {@link
   *     Subtypes#CONDITIONAL},{@link Subtypes#SORT},{@link Subtypes#GROUP_BY},{@link
   *     Subtypes#JOIN},{@link Subtypes#FILTER},
   * @param isMasking - is transformation masking (no indirect transformation is masking, but their
   *     dependencies can be)
   */
  public static TransformationInfo indirect(Subtypes subType, Boolean isMasking) {
    return new TransformationInfo(Types.INDIRECT, subType, "", isMasking);
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
  /**
   * Merges current {@link TransformationInfo} with another e.g. given two dependencies with
   * transformation types a -> b, t1 and b -> c, t2 the result of merge is transformation type for
   * dependency a -> c
   *
   * <pre> Rules applied here are:
   * 1. if current transformation is indirect, new type and subtype is taken from current
   * 2. if current is not indirect and another is null we can't deduce result so we return null
   * 3. if another is indirect, new type and subtype is taken from another
   * 4. otherwise the the type and subtype are taken from the one with lower {@link Subtypes#ordinal()}
   * 5. if any {@link TransformationInfo} has masking set to true, the result has masking set to true
   * </pre>
   *
   * @param another - {@link TransformationInfo} object to be merged with
   */
  public TransformationInfo merge(TransformationInfo another) {
    TransformationInfo res;
    if (Types.INDIRECT.equals(this.type)) {
      res = this;
    } else if (another != null) {
      if (Types.INDIRECT.equals(another.type)) {
        res = another;
      } else if (another.subType.ordinal() < this.subType.ordinal()) {
        res = another;
      } else {
        res = this;
      }
    } else {
      return null;
    }
    return new TransformationInfo(
        res.getType(),
        res.getSubType(),
        res.getDescription(),
        this.getMasking() || another.getMasking());
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

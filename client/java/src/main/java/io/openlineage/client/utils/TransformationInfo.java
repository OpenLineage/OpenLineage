/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import io.openlineage.client.OpenLineage;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;

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
    FILTER,
    WINDOW
  }

  @Getter private final Types type;
  @Getter private final Subtypes subType;
  @Getter private final String description;
  @Getter private final Boolean masking;

  private static final TransformationInfo TRANSFORMATION_IDENTITY =
      new TransformationInfo(Types.DIRECT, Subtypes.IDENTITY, "", false);

  private static final TransformationInfo TRANSFORMATION_MASKING =
      new TransformationInfo(Types.DIRECT, Subtypes.TRANSFORMATION, "", true);
  private static final TransformationInfo TRANSFORMATION_NON_MASKING =
      new TransformationInfo(Types.DIRECT, Subtypes.TRANSFORMATION, "", false);

  private static final TransformationInfo AGGREGATION_MASKING =
      new TransformationInfo(Types.DIRECT, Subtypes.AGGREGATION, "", true);
  private static final TransformationInfo AGGREGATION_NON_MASKING =
      new TransformationInfo(Types.DIRECT, Subtypes.AGGREGATION, "", false);

  private static final Map<Subtypes, TransformationInfo> INDIRECT_MASKING_MAP =
      Arrays.stream(Subtypes.values())
          .collect(
              Collectors.toMap(
                  subtype -> subtype,
                  subtype -> new TransformationInfo(Types.INDIRECT, subtype, "", true)));

  private static final Map<Subtypes, TransformationInfo> INDIRECT_NON_MASKING_MAP =
      Arrays.stream(Subtypes.values())
          .collect(
              Collectors.toMap(
                  subtype -> subtype,
                  subtype -> new TransformationInfo(Types.INDIRECT, subtype, "", false)));

  /**
   * Method that simplifies the creation of an {@link TransformationInfo} object representing {@link
   * Types#DIRECT}, {@link Subtypes#IDENTITY} transformation.
   */
  public static TransformationInfo identity() {
    return TRANSFORMATION_IDENTITY;
  }

  /**
   * Method that simplifies the creation of an {@link TransformationInfo} object representing
   * non-masking, {@link Types#DIRECT}, {@link Subtypes#TRANSFORMATION} transformation.
   */
  public static TransformationInfo transformation() {
    return TRANSFORMATION_NON_MASKING;
  }

  /**
   * Method that simplifies the creation of {@link TransformationInfo} object representing {@link
   * Types#DIRECT}, {@link Subtypes#TRANSFORMATION} transformation.
   *
   * @param isMasking - is transformation masking
   */
  public static TransformationInfo transformation(Boolean isMasking) {
    return isMasking ? TRANSFORMATION_MASKING : TRANSFORMATION_NON_MASKING;
  }

  /**
   * Method that simplifies the creation of an {@link TransformationInfo} object representing
   * non-masking, {@link Types#DIRECT}, {@link Subtypes#AGGREGATION} transformation.
   */
  public static TransformationInfo aggregation() {
    return AGGREGATION_NON_MASKING;
  }

  /**
   * Method that simplifies the creation of {@link TransformationInfo} object representing {@link
   * Types#DIRECT}, {@link Subtypes#AGGREGATION} transformation.
   *
   * @param isMasking - is transformation masking
   */
  public static TransformationInfo aggregation(Boolean isMasking) {
    return isMasking ? AGGREGATION_MASKING : AGGREGATION_NON_MASKING;
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
    return INDIRECT_NON_MASKING_MAP.get(subType);
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
    return isMasking ? INDIRECT_MASKING_MAP.get(subType) : INDIRECT_NON_MASKING_MAP.get(subType);
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
    if (this.getMasking().equals(another.getMasking())) {
      // no need to create new object
      return res;
    }
    return new TransformationInfo(res.getType(), res.getSubType(), res.getDescription(), true);
  }

  public OpenLineage.InputFieldTransformations toInputFieldsTransformations() {
    return new OpenLineage.InputFieldTransformationsBuilder()
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

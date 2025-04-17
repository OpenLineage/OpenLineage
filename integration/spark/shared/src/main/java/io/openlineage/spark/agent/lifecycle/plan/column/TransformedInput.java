/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import io.openlineage.client.utils.DatasetIdentifier;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
public class TransformedInput {
  @Getter @Setter Input input;
  @Getter @Setter TransformationInfo transformationInfo;

  public DatasetIdentifier getDatasetIdentifier() {
    return input.getDatasetIdentifier();
  }

  public String getName() {
    return input.getFieldName();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TransformedInput that = (TransformedInput) o;
    return Objects.equals(input, that.input)
        && Objects.equals(transformationInfo, that.transformationInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(input, transformationInfo);
  }
}

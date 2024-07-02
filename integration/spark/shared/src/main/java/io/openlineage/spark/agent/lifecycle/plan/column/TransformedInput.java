/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import io.openlineage.client.utils.DatasetIdentifier;
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
}

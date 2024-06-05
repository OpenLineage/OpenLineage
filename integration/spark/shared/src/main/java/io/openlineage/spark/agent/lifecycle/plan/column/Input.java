/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import io.openlineage.client.utils.DatasetIdentifier;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
public class Input {
  @Getter @Setter DatasetIdentifier datasetIdentifier;
  @Getter @Setter String name;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Input input = (Input) o;
    return Objects.equals(datasetIdentifier, input.datasetIdentifier)
        && Objects.equals(name, input.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(datasetIdentifier, name);
  }
}

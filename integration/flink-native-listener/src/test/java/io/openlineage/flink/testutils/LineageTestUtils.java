/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.testutils;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;

public class LineageTestUtils {

  @SneakyThrows
  public static List<RunEvent> fromFile(String eventFile) {
    return Files.readAllLines(Path.of(eventFile)).stream()
        .map(OpenLineageClientUtils::runEventFromJson)
        .collect(Collectors.toList());
  }

  public static List<InputDataset> getInputDatasets(List<RunEvent> events) {
    return events.stream()
        .filter(e -> e.getInputs() != null)
        .filter(e -> e.getInputs().size() > 0)
        .findAny()
        .map(e -> e.getInputs())
        .orElse(Collections.emptyList());
  }

  public static List<OutputDataset> getOutputDatasets(List<RunEvent> events) {
    return events.stream()
        .filter(e -> e.getOutputs() != null)
        .filter(e -> e.getOutputs().size() > 0)
        .findAny()
        .map(e -> e.getOutputs())
        .orElse(Collections.emptyList());
  }
}

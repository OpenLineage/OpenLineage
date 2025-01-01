/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Appends Openlineage events to a file specified. Events are separated by a newline character,
 * while all the existing newline characters within event json are removed. FileTransport was
 * introduced for the purpose of integration tests.
 */
@Slf4j
public class FileTransport extends Transport {

  File file;

  public FileTransport(@NonNull final FileConfig fileConfig) {
    file = new File(fileConfig.getLocation());
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    emit(OpenLineageClientUtils.toJson(runEvent));
  }

  @Override
  public void emit(@NonNull OpenLineage.DatasetEvent datasetEvent) {
    emit(OpenLineageClientUtils.toJson(datasetEvent));
  }

  @Override
  public void emit(@NonNull OpenLineage.JobEvent jobEvent) {
    emit(OpenLineageClientUtils.toJson(jobEvent));
  }

  private void emit(String eventAsJson) {
    try {
      Files.createDirectories(file.getParentFile().toPath());
      Files.write(
          file.toPath(),
          (eventAsJson.replace(System.lineSeparator(), "") + System.lineSeparator())
              .getBytes(StandardCharsets.UTF_8),
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
      log.debug("emitted event: " + eventAsJson);
    } catch (IOException | IllegalArgumentException e) {
      log.error("Writing event to a file {} failed: {}", file.getPath(), e);
    }
  }
}

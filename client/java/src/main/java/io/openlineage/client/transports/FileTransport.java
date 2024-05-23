/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

/**
 * Appends Openlineage events to a file specified. Events are separated by a newline character,
 * while all the existing newline characters within event json are removed. FileTransport was
 * introduced for the purpose of integration tests.
 */
@Slf4j
public class FileTransport extends Transport {

  File file;

  public FileTransport(@NonNull final FileConfig fileConfig) {
    super(Type.FILE);
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

  /**
   * @deprecated
   *     <p>Since version 1.13.0.
   *     <p>Will be removed in version 1.16.0.
   *     <p>Please use {@link #emit(OpenLineage.DatasetEvent)} or {@link
   *     #emit(OpenLineage.JobEvent)} instead
   */
  @Deprecated
  @Override
  public void emit(String eventAsJson) {
    try {
      FileUtils.writeStringToFile(
          file,
          eventAsJson.replace(System.lineSeparator(), "") + System.lineSeparator(),
          StandardCharsets.UTF_8,
          true);
      log.info("emitted event: " + eventAsJson);
    } catch (IOException | IllegalArgumentException e) {
      log.error("Writing event to a file {} failed: {}", file.getPath(), e);
    }
  }
}

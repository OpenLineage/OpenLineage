/*
/* Copyright 2018-2023 contributors to the OpenLineage project
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
  public void emit(OpenLineage.RunEvent runEvent) {
    // if DEBUG loglevel is enabled, this will double-log even due to OpenLineageClient also logging
    emit(OpenLineageClientUtils.toJson(runEvent));
  }

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

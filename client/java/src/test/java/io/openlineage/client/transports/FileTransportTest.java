/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import java.io.File;
import java.net.URI;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FileTransportTest {

  private static final String FILE_LOCATION_DIR = "/tmp/openlineage_transport_test";
  private static final String FILE_LOCATION = FILE_LOCATION_DIR + "/events.log";

  FileConfig fileConfig;
  Transport transport;

  @BeforeEach
  @SneakyThrows
  public void beforeEach() {
    FileUtils.deleteDirectory(new File(FILE_LOCATION_DIR));

    fileConfig = new FileConfig();
    fileConfig.setLocation(FILE_LOCATION);
    transport = new FileTransport(fileConfig);
  }

  @Test
  @SneakyThrows
  public void transportAppendsToFile() {
    transport.emit(
        new OpenLineage(URI.create("http://test.producer"))
            .newRunEventBuilder()
            .job(new OpenLineage.JobBuilder().name("test-job").namespace("test-ns").build())
            .build());
    List<String> lines = FileUtils.readLines(new File(FILE_LOCATION));

    assertThat(lines.size()).isEqualTo(1);
    assertThat(lines.get(0)).contains("test-job");
  }

  @Test
  @SneakyThrows
  public void transportCannotAppendToFileWhenFileNotWriteable() {
    fileConfig = new FileConfig();
    fileConfig.setLocation(FILE_LOCATION);
    transport = new FileTransport(fileConfig);

    transport.emit("{some-event}");

    // make file unwritable
    new File(FILE_LOCATION).setWritable(false);

    // should not be written
    transport.emit("{some-event}");

    assertThat(FileUtils.readLines(new File(FILE_LOCATION)).size()).isEqualTo(1);
  }

  @Test
  @SneakyThrows
  public void multipleEventsAreSeparatedByNewline() {
    transport.emit("{some-event-1}");
    transport.emit("{some-event-2}");

    List<String> lines = FileUtils.readLines(new File(FILE_LOCATION));

    assertThat(lines.size()).isEqualTo(2);
    assertThat(lines.get(0)).isEqualTo("{some-event-1}");
    assertThat(lines.get(1)).isEqualTo("{some-event-2}");
  }

  @Test
  @SneakyThrows
  public void newlinesAreRemovedFromWrittenEvents() {
    transport.emit("some-event-\n-same-event");
    List<String> lines = FileUtils.readLines(new File(FILE_LOCATION));

    assertThat(lines.size()).isEqualTo(1);
    assertThat(lines.get(0)).isEqualTo("some-event--same-event");
  }
}

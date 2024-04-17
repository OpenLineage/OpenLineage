/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static io.openlineage.client.Events.datasetEvent;
import static io.openlineage.client.Events.jobEvent;
import static io.openlineage.client.Events.runEvent;
import static io.openlineage.client.Events.runEventWithParent;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.File;
import java.net.URI;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FileTransportTest {

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
  void transportEmitsRunEvent() {
    OpenLineage.RunEvent event = runEvent();
    String eventSerialized = OpenLineageClientUtils.toJson(event);

    transport.emit(event);

    List<String> lines = FileUtils.readLines(new File(FILE_LOCATION));

    assertThat(lines.size()).isEqualTo(1);
    assertThat(lines.get(0)).isEqualTo(eventSerialized);
  }

  @Test
  @SneakyThrows
  void transportEmitsDatasetEvent() {
    OpenLineage.DatasetEvent event = datasetEvent();
    String eventSerialized = OpenLineageClientUtils.toJson(event);

    transport.emit(event);

    List<String> lines = FileUtils.readLines(new File(FILE_LOCATION));

    assertThat(lines.size()).isEqualTo(1);
    assertThat(lines.get(0)).isEqualTo(eventSerialized);
  }

  @Test
  @SneakyThrows
  void transportEmitsJobEvent() {
    OpenLineage.JobEvent event = jobEvent();
    String eventSerialized = OpenLineageClientUtils.toJson(event);

    transport.emit(event);

    List<String> lines = FileUtils.readLines(new File(FILE_LOCATION));

    assertThat(lines.size()).isEqualTo(1);
    assertThat(lines.get(0)).isEqualTo(eventSerialized);
  }

  @Test
  @SneakyThrows
  void transportCannotAppendToFileWhenFileNotWriteable() {
    fileConfig = new FileConfig();
    fileConfig.setLocation(FILE_LOCATION);
    transport = new FileTransport(fileConfig);

    transport.emit(runEvent());

    // make file unwritable
    new File(FILE_LOCATION).setWritable(false);

    // should not be written
    transport.emit(runEvent());

    assertThat(FileUtils.readLines(new File(FILE_LOCATION)).size()).isEqualTo(1);
  }

  @Test
  @SneakyThrows
  void multipleEventsAreSeparatedByNewline() {
    OpenLineage.RunEvent event = runEvent();
    OpenLineage.RunEvent anotherEvent = runEventWithParent();

    transport.emit(event);
    transport.emit(anotherEvent);

    List<String> lines = FileUtils.readLines(new File(FILE_LOCATION));

    String eventSerialized = OpenLineageClientUtils.toJson(event);
    String anotherEventSerialized = OpenLineageClientUtils.toJson(anotherEvent);

    assertThat(lines.size()).isEqualTo(2);
    assertThat(lines.get(0)).isEqualTo(eventSerialized);
    assertThat(lines.get(1)).isEqualTo(anotherEventSerialized);
  }

  @Test
  @SneakyThrows
  void newlinesAreRemovedFromWrittenEvents() {
    OpenLineage.RunEvent event =
        new OpenLineage(URI.create("http://test.producer"))
            .newRunEventBuilder()
            .job(new OpenLineage.JobBuilder().name("test-\n-job").namespace("test-\n-ns").build())
            .build();

    transport.emit(event);
    List<String> lines = FileUtils.readLines(new File(FILE_LOCATION));

    assertThat(lines.size()).isEqualTo(1);
    assertThat(lines.get(0))
        .isEqualTo(
            "{\"producer\":\"http://test.producer\",\"schemaURL\":\"https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent\",\"job\":{\"namespace\":\"test-\\n-ns\",\"name\":\"test-\\n-job\"}}");
  }
}

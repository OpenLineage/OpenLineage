/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.client;

import io.openlineage.client.Clients;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageConfig;
import io.openlineage.integration.identity.DatasetIdentityNormalizer;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventEmitter {

  private final OpenLineageClient client;
  private final DatasetIdentityNormalizer identityNormalizer;

  public static final URI OPEN_LINEAGE_CLIENT_URI = getUri();
  public static final String OPEN_LINEAGE_PARENT_FACET_URI =
      "https://openlineage.io/spec/1-0-1/OpenLineage.json#/definitions/ParentRunFacet";
  public static final String OPEN_LINEAGE_DATASOURCE_FACET =
      "https://openlineage.io/spec/1-0-1/OpenLineage.json#/definitions/DatasourceDatasetFacet";
  public static final String OPEN_LINEAGE_SCHEMA_FACET_URI =
      "https://openlineage.io/spec/1-0-1/OpenLineage.json#/definitions/SchemaDatasetFacet";

  public EventEmitter(OpenLineageConfig openLineageConfig) {
    this.identityNormalizer =
        DatasetIdentityNormalizer.fromTransport(openLineageConfig.getTransportConfig());
    if (openLineageConfig.getTransportConfig() != null) {
      // build emitter client based on flink configuration
      this.client = Clients.newClient(openLineageConfig);
    } else {
      // build emitter default way - openlineage.yml file or system properties
      client = Clients.newClient();
    }
  }

  public synchronized void emit(OpenLineage.RunEvent event) {
    try {
      for (OpenLineage.RunEvent ready : normalize(event)) client.emit(ready);
    } catch (OpenLineageClientException | IOException | IllegalArgumentException exception) {
      log.error("Failed to emit OpenLineage event: ", exception);
    }
  }

  public synchronized void emit(OpenLineage.RunEvent event, Duration timeout) {
    try {
      for (OpenLineage.RunEvent ready : normalize(event)) client.emit(ready, timeout);
    } catch (OpenLineageClientException | IOException | IllegalArgumentException exception) {
      log.error("Failed to emit OpenLineage event: ", exception);
    }
  }

  private List<OpenLineage.RunEvent> normalize(OpenLineage.RunEvent event) throws IOException {
    return identityNormalizer == null
        ? Collections.singletonList(event)
        : identityNormalizer.normalizeEvents(event, null);
  }

  public synchronized void close() {
    try {
      client.close();
    } catch (Exception e) {
      log.error("Failed to close OpenLineage client", e);
    }
  }

  private static URI getUri() {
    return URI.create(
        String.format(
            "https://github.com/OpenLineage/OpenLineage/tree/%s/integration/flink", getVersion()));
  }

  private static String getVersion() {
    try {
      Properties properties = new Properties();
      InputStream is = EventEmitter.class.getResourceAsStream("version.properties");
      properties.load(is);
      return properties.getProperty("version");
    } catch (IOException exception) {
      return "main";
    }
  }
}

/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.client;

import io.openlineage.client.Clients;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.transports.TransportFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventEmitter {
  private final OpenLineageClient client;
  public static final URI OPEN_LINEAGE_CLIENT_URI = getUri();
  public static final String OPEN_LINEAGE_PARENT_FACET_URI =
      "https://openlineage.io/spec/1-0-1/OpenLineage.json#/definitions/ParentRunFacet";
  public static final String OPEN_LINEAGE_DATASOURCE_FACET =
      "https://openlineage.io/spec/1-0-1/OpenLineage.json#/definitions/DatasourceDatasetFacet";
  public static final String OPEN_LINEAGE_SCHEMA_FACET_URI =
      "https://openlineage.io/spec/1-0-1/OpenLineage.json#/definitions/SchemaDatasetFacet";

  public EventEmitter(OpenLineageConfig openLineageConfig) {
    if (openLineageConfig.getTransportConfig() != null) {
      // build emitter client based on flink configuration
      this.client =
          OpenLineageClient.builder()
              .transport(new TransportFactory(openLineageConfig.getTransportConfig()).build())
              .build();
    } else {
      // build emitter default way - openlineage.yml file or system properties
      log.info("No transport config provided - building default transport");
      client = Clients.newClient();
    }
  }

  public void emit(OpenLineage.RunEvent event) {
    try {
      client.emit(event);
    } catch (OpenLineageClientException exception) {
      log.error("Failed to emit OpenLineage event: ", exception);
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

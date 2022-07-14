package io.openlineage.flink.agent.client;

import io.openlineage.client.Clients;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

public class EventEmitter {

  private final OpenLineageClient client;

  public static final URI OPEN_LINEAGE_CLIENT_URI = getUri();
  public static final String OPEN_LINEAGE_PARENT_FACET_URI =
      "https://openlineage.io/spec/1-0-1/OpenLineage.json#/definitions/ParentRunFacet";
  public static final String OPEN_LINEAGE_DATASOURCE_FACET =
      "https://openlineage.io/spec/1-0-1/OpenLineage.json#/definitions/DatasourceDatasetFacet";
  public static final String OPEN_LINEAGE_SCHEMA_FACET_URI =
      "https://openlineage.io/spec/1-0-1/OpenLineage.json#/definitions/SchemaDatasetFacet";

  public EventEmitter() {
    client = Clients.newClient();
  }

  public void emit(OpenLineage.RunEvent event) {
    client.emit(event);
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

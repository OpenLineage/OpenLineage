package io.openlineage.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.transports.TransportConfig;
import lombok.Getter;

/** Configuration for {@link OpenLineageClient}. */
public class OpenLineageYaml {
  @Getter
  @JsonProperty("transport")
  private TransportConfig transportConfig;
}

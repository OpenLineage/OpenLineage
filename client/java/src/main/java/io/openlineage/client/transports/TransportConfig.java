package io.openlineage.client.transports;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonSubTypes({
  @JsonSubTypes.Type(value = ConsoleConfig.class, name = "Console"),
  @JsonSubTypes.Type(value = HttpConfig.class, name = "HTTP"),
  @JsonSubTypes.Type(value = KafkaConfig.class, name = "Kafka")
})
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public interface TransportConfig {}

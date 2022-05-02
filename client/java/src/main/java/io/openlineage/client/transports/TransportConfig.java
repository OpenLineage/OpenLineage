package io.openlineage.client.transports;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonSubTypes({
  @JsonSubTypes.Type(value = ConsoleConfig.class, name = "console"),
  @JsonSubTypes.Type(value = HttpConfig.class, name = "http"),
  @JsonSubTypes.Type(value = KafkaConfig.class, name = "kafka")
})
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public interface TransportConfig {}

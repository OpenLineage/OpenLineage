package io.openlineage.proxy;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.openlineage.proxy.api.models.ConsoleConfig;
import io.openlineage.proxy.api.models.KafkaConfig;

@JsonSubTypes({
  @JsonSubTypes.Type(value = ConsoleConfig.class, name = "Console"),
  @JsonSubTypes.Type(value = KafkaConfig.class, name = "Kafka")
})
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public interface ProxyStreamConfig {}

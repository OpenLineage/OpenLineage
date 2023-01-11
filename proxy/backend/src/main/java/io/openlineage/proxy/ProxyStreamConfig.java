/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.proxy;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.openlineage.proxy.api.models.ConsoleConfig;
import io.openlineage.proxy.api.models.HttpConfig;
import io.openlineage.proxy.api.models.KafkaConfig;

@JsonSubTypes({
  @JsonSubTypes.Type(value = ConsoleConfig.class, name = "Console"),
  @JsonSubTypes.Type(value = KafkaConfig.class, name = "Kafka"),
  @JsonSubTypes.Type(value = HttpConfig.class, name = "Http")
})
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public interface ProxyStreamConfig {}

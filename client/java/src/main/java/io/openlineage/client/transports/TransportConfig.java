/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.Properties;

@JsonSubTypes({
  @JsonSubTypes.Type(value = ConsoleConfig.class, name = "console"),
  @JsonSubTypes.Type(value = HttpConfig.class, name = "http"),
  @JsonSubTypes.Type(value = KafkaConfig.class, name = "kafka"),
  @JsonSubTypes.Type(value = KafkaConfig.class, name = "kinesis")
})
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@NoArgsConstructor
@AllArgsConstructor
@ToString
public abstract class TransportConfig {
    @Getter @Setter protected Properties properties;
}

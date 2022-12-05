/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import java.util.Optional;
import java.util.Properties;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@ToString
public final class KinesisConfig implements TransportConfig {
  @Getter @Setter private String streamName;
  @Getter @Setter private String region;
  @Getter @Setter private String roleArn;

  // check
  // https://github.com/awslabs/amazon-kinesis-producer/blob/master/java/amazon-kinesis-producer-sample/default_config.properties
  @Getter @Setter private Properties properties;
  
  
}

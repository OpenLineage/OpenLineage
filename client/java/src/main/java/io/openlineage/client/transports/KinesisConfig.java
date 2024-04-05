/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.MergeConfig;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@ToString
public final class KinesisConfig implements TransportConfig, MergeConfig<KinesisConfig> {
  @Getter @Setter private String streamName;
  @Getter @Setter private String region;
  @Getter @Setter private String roleArn;

  // check
  // https://github.com/awslabs/amazon-kinesis-producer/blob/master/java/amazon-kinesis-producer-sample/default_config.properties
  @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
  @Getter
  @Setter
  private Properties properties;

  @Override
  public KinesisConfig mergeWithNonNull(KinesisConfig other) {
    Properties p = new Properties();
    p.putAll(mergePropertyWith(properties, other.properties));

    return new KinesisConfig(
        mergePropertyWith(streamName, other.streamName),
        mergePropertyWith(region, other.region),
        mergePropertyWith(roleArn, other.roleArn),
        p);
  }
}

/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.KafkaConfig;
import io.openlineage.client.transports.KinesisConfig;
import io.openlineage.client.transports.TransportConfig;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.net.URLEncodedUtils;
import org.apache.spark.SparkConf;
import org.jetbrains.annotations.NotNull;

import static io.openlineage.spark.agent.util.SparkConfUtils.findSparkConfigKey;
import static io.openlineage.spark.agent.util.SparkConfUtils.findSparkConfigKeyDouble;
import static io.openlineage.spark.agent.util.SparkConfUtils.findSparkConfigKeysStartsWith;
import static io.openlineage.spark.agent.util.SparkConfUtils.findSparkUrlParams;

@AllArgsConstructor
@Slf4j
@Getter
@ToString
@Builder
public class ArgumentParser {

  public static final String SPARK_CONF_TRANSPORT_TYPE = "openlineage.transport.type";
  public static final String SPARK_CONF_CONSOLE_TRANSPORT = "openlineage.consoleTransport";
  public static final String SPARK_CONF_URL = "openlineage.url";
  public static final String SPARK_CONF_HOST = "openlineage.host";
  public static final String SPARK_CONF_API_VERSION = "openlineage.version";
  public static final String SPARK_CONF_NAMESPACE = "openlineage.namespace";
  public static final String SPARK_CONF_JOB_NAME = "openlineage.parentJobName";
  public static final String SPARK_CONF_PARENT_RUN_ID = "openlineage.parentRunId";
  public static final String SPARK_CONF_TIMEOUT = "openlineage.timeout";
  public static final String SPARK_CONF_API_KEY = "openlineage.apiKey";
  public static final String SPARK_CONF_URL_PARAM_PREFIX = "openlineage.url.param";
  private static final String SPARK_CONF_APP_NAME = "openlineage.appName";
  private static final String SPARK_CONF_FACETS_DISABLED = "openlineage.facets.disabled";
  
  
  
  public static final Set<String> namedParams =
      new HashSet<>(Arrays.asList("timeout", "api_key", "app_name"));
  public static final String disabledFacetsSeparator = ";";

  @Builder.Default private String host = "";
  @Builder.Default private String version = "v1";
  @Builder.Default private String namespace = "default";
  @Builder.Default private String jobName = "default";
  @Builder.Default private String parentRunId = null;
  @Builder.Default private Optional<Double> timeout = Optional.empty();
  @Builder.Default private Optional<String> apiKey = Optional.empty();
  @Builder.Default private Optional<String> appName = Optional.empty();
  @Builder.Default private Optional<Map<String, String>> urlParams = Optional.empty();
  @Builder.Default private String disabledFacets = "spark_unknown";
  @Builder.Default private boolean consoleMode = false;

  @Builder.Default private Optional<TransportConfig> transportConfig = Optional.empty();
  @Builder.Default private Optional<String> transportMode = Optional.empty();

  public static ArgumentParserBuilder parseUrl(String clientUrl) {
    return UrlParser.parseUrl(clientUrl);
  }

  private void commonConfigParse(ArgumentParser.ArgumentParserBuilder builder, SparkConf conf) {
    builder

            .appName(findSparkConfigKey(conf, SPARK_CONF_APP_NAME).filter(str -> !str.isEmpty()));
            
    builder      
            .timeout(findSparkConfigKeyDouble(conf, SPARK_CONF_TIMEOUT))
            .apiKey(findSparkConfigKey(conf, SPARK_CONF_API_KEY).filter(str -> !str.isEmpty()))
            .urlParams(findSparkUrlParams(conf, SPARK_CONF_URL_PARAM_PREFIX));

    findSparkConfigKey(conf, SPARK_CONF_FACETS_DISABLED).ifPresent(builder::disabledFacets);
    findSparkConfigKey(conf, SPARK_CONF_HOST).ifPresent(builder::host);
    findSparkConfigKey(conf, SPARK_CONF_API_VERSION).ifPresent(builder::version);
    
    
    
    
    findSparkConfigKey(conf, SPARK_CONF_NAMESPACE).ifPresent(builder::namespace);
    findSparkConfigKey(conf, SPARK_CONF_JOB_NAME).ifPresent(builder::jobName);
    findSparkConfigKey(conf, SPARK_CONF_PARENT_RUN_ID).ifPresent(builder::parentRunId);
  }



  private ArgumentParser parseConf(SparkConf conf) {
    Optional<String> transportType = findSparkConfigKey(conf, SPARK_CONF_TRANSPORT_TYPE);
    ArgumentParser.ArgumentParserBuilder builder;
    if (transportType.isPresent()){
      String mode = transportType.get().toLowerCase();
      Optional<TransportConfig> transportConfig = Optional.empty();
      switch (mode) {
        case "kinesis":
          transportConfig = getKinesisTransportConfig(conf);
          break;
        case "kafka":
          transportConfig = getKafkaTransportConfig(conf);
          break;
        case "http":
          log.info("dupa");
          break;
      }
      builder.transportMode(Optional.of(mode)).transportConfig(transportConfig);
    }
    else {
      boolean consoleMode =
              findSparkConfigKey(conf, SPARK_CONF_CONSOLE_TRANSPORT)
                      .map(Boolean::valueOf)
                      .filter(v -> v)
                      .orElse(false);
      builder.consoleMode(consoleMode);
    }
    return builder.build();
  }

  @NotNull
  private static Optional<TransportConfig> getKafkaTransportConfig(SparkConf conf) {
    Optional<TransportConfig> transportConfig;
    Map<String, String> config =
            findSparkConfigKeysStartsWith(conf, "spark.openlineage.transport.kafka.");
    KafkaConfig kafkaConfig = new KafkaConfig();
    kafkaConfig.setLocalServerId("localServerId");
    kafkaConfig.setTopicName("topic");
    Properties properties = new Properties();
    properties.putAll(config);
    kafkaConfig.setProperties(properties);
    transportConfig = Optional.of(kafkaConfig);
    return transportConfig;
  }

  @NotNull
  private static Optional<TransportConfig> getKinesisTransportConfig(SparkConf conf) {
    Optional<TransportConfig> transportConfig;
    Map<String, String> config =
            findSparkConfigKeysStartsWith(conf, "spark.openlineage.transport.kinesis.");
    KinesisConfig kinesisConfig = new KinesisConfig();
    kinesisConfig.setStreamName(config.get("streamName"));
    kinesisConfig.setRegion(config.get("region"));
    kinesisConfig.setRoleArn(Optional.ofNullable(config.get("roleArn")));
    Properties properties = new Properties();
    properties.putAll(config);
    kinesisConfig.setProperties(properties);
    transportConfig = Optional.of(kinesisConfig);
    return transportConfig;
  }


  @NotNull
  private static Optional<TransportConfig> getHttpTransportConfig(SparkConf conf) {
    Optional<TransportConfig> transportConfig;
    Map<String, String> config =
            findSparkConfigKeysStartsWith(conf, "spark.openlineage.transport.http.");
    HttpConfig httpConfig = new HttpConfig();
    httpConfig.setUrl(new URI(config.get("url")));
    httpConfig.setEndpoint(config.get("endpoint"));
    httpConfig.setTimeout(config.get("timeout"));
    httpConfig.setAuth(config.get("auth"));
    transportConfig = Optional.of(kafkaConfig);
    return transportConfig;
  }
  public static ArgumentParserBuilder builder() {
    return new ArgumentParserBuilder() {
      @Override
      public ArgumentParser build() {
        ArgumentParser argumentParser = super.build();
        log.info(
            String.format(
                "%s/api/%s/namespaces/%s/jobs/%s/runs/%s",
                argumentParser.getHost(),
                argumentParser.getVersion(),
                argumentParser.getNamespace(),
                argumentParser.getJobName(),
                argumentParser.getParentRunId()));
        return argumentParser;
      }
    };
  }
}

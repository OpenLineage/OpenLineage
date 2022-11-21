/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import io.openlineage.client.transports.*;

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

  private static final String TRANSPORT_PREFIX = "spark.openlineage.transport";
  
  
  
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

  @Builder.Default private TransportConfig transportConfig = new ConsoleConfig();
  @Builder.Default private String transportMode = "console";

  public static ArgumentParserBuilder parseUrl(String clientUrl) {
    return UrlParser.parseUrl(clientUrl);
  }

  private void commonConfigParse(ArgumentParser.ArgumentParserBuilder builder, SparkConf conf) {
    builder
            .timeout(findSparkConfigKeyDouble(conf, SPARK_CONF_TIMEOUT))
            .apiKey(findSparkConfigKey(conf, SPARK_CONF_API_KEY).filter(str -> !str.isEmpty()))
            .urlParams(findSparkUrlParams(conf, SPARK_CONF_URL_PARAM_PREFIX));
    findSparkConfigKey(conf, SPARK_CONF_HOST).ifPresent(builder::host);
    findSparkConfigKey(conf, SPARK_CONF_API_VERSION).ifPresent(builder::version);
  }



  private ArgumentParser parseConf(SparkConf conf) {
    Optional<String> transportType = findSparkConfigKey(conf, SPARK_CONF_TRANSPORT_TYPE);
    ArgumentParser.ArgumentParserBuilder builder;
      String mode = transportType.orElse("console").toLowerCase();
      switch (mode) {
        case "kinesis":
          builder = getKinesisTransportConfig(conf);
          break;
        case "kafka":
          builder = getKafkaTransportConfig(conf);
          break;
        case "http":
          builder = getHttpTransportConfig(conf);
          break;
        default:
          builder = getConsoleTransportConfig(conf);
      }
    return builder.build();
  }

  private static ArgumentParserBuilder parseCommonConfig(SparkConf conf) {
    ArgumentParserBuilder builder = new ArgumentParserBuilder();
    builder.appName(findSparkConfigKey(conf, SPARK_CONF_APP_NAME).filter(str -> !str.isEmpty()));
    findSparkConfigKey(conf, SPARK_CONF_NAMESPACE).ifPresent(builder::namespace);
    findSparkConfigKey(conf, SPARK_CONF_JOB_NAME).ifPresent(builder::jobName);
    findSparkConfigKey(conf, SPARK_CONF_PARENT_RUN_ID).ifPresent(builder::parentRunId);
    findSparkConfigKey(conf, SPARK_CONF_FACETS_DISABLED).ifPresent(builder::disabledFacets);
    return builder;
  }

  private ArgumentParserBuilder getBuilder(SparkConf conf){
    ArgumentParserBuilder builder = parseCommonConfig(conf);
    Optional<String> transportType = findSparkConfigKey(conf, SPARK_CONF_TRANSPORT_TYPE);
    String mode = transportType.orElse("console").toLowerCase();
    Map<String, String> config =
            findSparkConfigKeysStartsWith(conf, TRANSPORT_PREFIX, mode);
    Properties properties = new Properties();
    properties.putAll(config);

    TransportConfig transportConfig;
    switch (mode) {
      case "kinesis":
        transportConfig = getKafkaConfig(config);
        ((KinesisConfig)transportConfig).setProperties(properties);
        break;
      case "kafka":
        transportConfig = getKafkaConfig(config);
        ((KafkaConfig)transportConfig).setProperties(properties);
        break;
      case "http":
        transportConfig = getKafkaConfig(config);
        ((HttpConfig)transportConfig).setProperties(properties);
        break;
      default:
        transportConfig = getKafkaConfig(config);
    }


  }

  @NotNull
  private static ArgumentParserBuilder getKafkaTransportConfig(SparkConf conf) {
    ArgumentParserBuilder builder = parseCommonConfig(conf);
    String mode = "kafka";
    Map<String, String> config =
            findSparkConfigKeysStartsWith(conf, TRANSPORT_PREFIX, mode);
    KafkaConfig kafkaConfig = getKafkaConfig(config);
    Properties properties = new Properties();
    properties.putAll(config);
    kafkaConfig.setProperties(properties);
    builder.transportMode(mode).transportConfig(kafkaConfig);
    return builder;
  }

  @NotNull
  private static KafkaConfig getKafkaConfig(Map<String, String> config) {
    KafkaConfig kafkaConfig = new KafkaConfig();
    kafkaConfig.setLocalServerId(config.get("localServerId"));
    kafkaConfig.setTopicName(config.get("topic"));
    return kafkaConfig;
  }

  @NotNull
  private static Optional<TransportConfig> getKinesisTransportConfig(SparkConf conf) {
    Optional<TransportConfig> transportConfig;
    Map<String, String> config =
        findSparkConfigKeysStartsWith(conf, TRANSPORT_PREFIX, "kafka");
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

  private static Optional<TransportConfig> getConsoleTransportConfig(SparkConf conf) {
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

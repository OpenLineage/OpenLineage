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
  public static final String SPARK_CONF_URL = "spark.openlineage.transport.http.url";
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
  private static final String OPENLINEAGE_PREFIX = "spark.openlineage";
  
  
  
  
  public static final Set<String> namedParams =
      new HashSet<>(Arrays.asList("timeout", "api_key", "app_name"));
  public static final String disabledFacetsSeparator = ";";


  @Builder.Default private String namespace = "default";
  @Builder.Default private String jobName = "default";
  @Builder.Default private String parentRunId = null;
  @Builder.Default private String appName = null;
  @Builder.Default private String disabledFacets = "spark_unknown";
  @Builder.Default private TransportConfig transportConfig = new ConsoleConfig();
  @Builder.Default private String transportMode = "console";
  
  public static ArgumentParser parseConf(SparkConf conf) {
    return getBuilder(conf).build();
  }

  private static ArgumentParserBuilder parseCommonConfig(SparkConf conf) {
    ArgumentParserBuilder builder = new ArgumentParserBuilder();
    findSparkConfigKey(conf, SPARK_CONF_APP_NAME).filter(str -> !str.isEmpty()).ifPresent(builder::namespace);
    findSparkConfigKey(conf, SPARK_CONF_NAMESPACE).ifPresent(builder::namespace);
    findSparkConfigKey(conf, SPARK_CONF_JOB_NAME).ifPresent(builder::jobName);
    findSparkConfigKey(conf, SPARK_CONF_PARENT_RUN_ID).ifPresent(builder::parentRunId);
    findSparkConfigKey(conf, SPARK_CONF_FACETS_DISABLED).ifPresent(builder::disabledFacets);
    return builder;
  }

  private static ArgumentParserBuilder getBuilder(SparkConf conf){
    ArgumentParserBuilder builder = parseCommonConfig(conf);
    Optional<String> transportType = findSparkConfigKey(conf, SPARK_CONF_TRANSPORT_TYPE);
    String mode = transportType.orElse("console").toLowerCase();
    Map<String, String> config =
            findSparkConfigKeysStartsWith(conf, TRANSPORT_PREFIX, mode);
    TransportConfig transportConfig;
    switch (mode) {
      case "kinesis":
        transportConfig = getKafkaConfig(config);
        break;
      case "kafka":
        transportConfig = getKinesisConfig(config);
        break;
      case "http":
        if(config.containsKey("http.url")){
          builder = UrlParser.parseUrl(builder, config.get("http.url"));
        }else{
          transportConfig = getHttpConfig(config);
        }
        
        break;
      default:
        transportConfig = getConsoleConfig(config);
    }
    if(!config.containsKey("http.url")){
      Properties properties = new Properties();
      properties.putAll(config);
      transportConfig.setProperties(properties);
    }
    
    builder.transportConfig(transportConfig);
    builder.transportMode(mode);
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
  private static KinesisConfig getKinesisConfig(Map<String, String> config) {
    KinesisConfig kinesisConfig = new KinesisConfig();
    kinesisConfig.setStreamName(config.get("streamName"));
    kinesisConfig.setRegion(config.get("region"));
    kinesisConfig.setRoleArn(Optional.ofNullable(config.get("roleArn")));
    return kinesisConfig;
  } 
  
  @NotNull
  private static HttpConfig getHttpConfig(Map<String, String> config) {
    HttpConfig httpConfig = new HttpConfig();
    
    httpConfig.setEndpoint(config.get("endpoint"));
    httpConfig.setTimeout(findSparkConfigKeyDouble(config, "timeout"));
    httpConfig.setAuth(config.get("auth"));
    return httpConfig;
  }

  private static TransportConfig getConsoleConfig(Map<String, String> config) {
    return new ConsoleConfig();
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

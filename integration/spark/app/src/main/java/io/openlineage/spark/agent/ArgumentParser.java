/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.OpenLineageYaml;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;

import io.openlineage.client.transports.KinesisConfig;
import io.openlineage.client.transports.TransportConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.net.URLEncodedUtils;
import org.apache.spark.SparkConf;
import org.json.JSONArray;
import org.json.JSONObject;
import scala.Tuple2;

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

  public static final String SPARK_CONF_NAMESPACE = "spark.openlineage.namespace";
  public static final String SPARK_CONF_JOB_NAME = "spark.openlineage.parentJobName";
  public static final String SPARK_CONF_PARENT_RUN_ID = "spark.openlineage.parentRunId";
  public static final String SPARK_CONF_APP_NAME = "spark.openlineage.appName";
  public static final String DISABLED_FACETS_SEPARATOR = ";";
  public static final String SPARK_CONF_TRANSPORT_TYPE = "spark.openlineage.transport.type";
  public static final String SPARK_CONF_HTTP_URL = "spark.openlineage.transport.http.url";
  
  @Builder.Default private String namespace = "default";
  @Builder.Default private String jobName = "default";
  @Builder.Default private String parentRunId = null;
  @Builder.Default private String appName = null;
  @Builder.Default private String DEFAULT_DISABLED_FACETS = "spark_unknown";

  @Builder.Default private OpenLineageYaml openLineageYaml = new OpenLineageYaml();
  
  public static ArgumentParser parse(SparkConf conf){
    ArgumentParserBuilder builder = ArgumentParser.builder();
    Optional<String> transportType = findSparkConfigKey(conf, SPARK_CONF_TRANSPORT_TYPE);
    if (!transportType.isPresent()) { 
      conf.set(SPARK_CONF_TRANSPORT_TYPE, "console");
    }
    
    if(transportType.orElseGet(()->"console").equals("http")){
      findSparkConfigKey(conf, SPARK_CONF_HTTP_URL)
              .ifPresent(url -> UrlParser.parseUrl(url).forEach(conf::set));
    }
    findSparkConfigKey(conf, SPARK_CONF_APP_NAME).filter(str -> !str.isEmpty()).ifPresent(builder::appName);
    findSparkConfigKey(conf, SPARK_CONF_NAMESPACE).ifPresent(builder::namespace);
    findSparkConfigKey(conf, SPARK_CONF_JOB_NAME).ifPresent(builder::jobName);
    findSparkConfigKey(conf, SPARK_CONF_PARENT_RUN_ID).ifPresent(builder::parentRunId);
    
    return builder.build();
  }

  public static OpenLineageYaml extractOpenlineageConfFromSparkConf(SparkConf conf) {
    Tuple2<String, String>[] olconf = conf.getAllWithPrefix("spark.openlineage.");
    JSONObject jsonObject = new JSONObject();
    for(Tuple2<String, String> tuple: olconf){
      JSONObject temp = jsonObject;
      String[] jpath = tuple._1.split("\\.");
      Iterator<String> iter = Arrays.stream(jpath).iterator();
      boolean leaf = false;
      while(!leaf){
        String key = iter.next();
        if(iter.hasNext()){
          if(!temp.has(key)){
            temp.put(key, new JSONObject());
          }
          temp = temp.getJSONObject(key);
        }
        else{
          if(tuple._2.contains(";")) {
            JSONArray jsonArray = new JSONArray();
            Arrays.stream(tuple._2.split(";")).forEach(jsonArray::put);
            temp.put(key, jsonArray);
          }
          else{
            temp.put(key, tuple._2);
            
          }
          leaf = true;
        }
      }
    }
    return OpenLineageClientUtils.loadOpenLineageYaml(new ByteArrayInputStream(jsonObject.toString().getBytes()));
  }
}

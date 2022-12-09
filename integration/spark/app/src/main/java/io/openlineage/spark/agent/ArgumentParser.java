/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.util.SparkConfUtils.findSparkConfigKey;

import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.OpenLineageYaml;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.json.JSONArray;
import org.json.JSONObject;
import scala.Tuple2;

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
  public static final String SPARK_CONF_DISABLED_FACETS = "spark.openlineage.facets.disabled";
  public static final String DEFAULT_DISABLED_FACETS = "spark_unknown;";
  public static final String DISABLED_FACETS_SEPARATOR = ";";
  public static final String SPARK_CONF_TRANSPORT_TYPE = "spark.openlineage.transport.type";
  public static final String SPARK_CONF_HTTP_URL = "spark.openlineage.transport.url";
  public static final Set<String> PROPERTIES_PREFIXES =
      new HashSet<>(Arrays.asList("transport.properties.", "transport.urlParams."));

  @Builder.Default private String namespace = "default";
  @Builder.Default private String jobName = "default";
  @Builder.Default private String parentRunId = null;
  @Builder.Default private String appName = null;
  @Builder.Default private OpenLineageYaml openLineageYaml = new OpenLineageYaml();

  public static ArgumentParser parse(SparkConf conf) {
    ArgumentParserBuilder builder = ArgumentParser.builder();
    conf.setIfMissing(SPARK_CONF_DISABLED_FACETS, DEFAULT_DISABLED_FACETS);
    conf.setIfMissing(SPARK_CONF_TRANSPORT_TYPE, "console");
    
    log.info("OPENLINEAGE CONFIG PARAMETERS");
    Arrays.stream(conf.getAllWithPrefix("spark.openlineage.")).forEach(e->log.info("OL PARAM: " + e._1 + " = " + e._2));
    if (conf.get(SPARK_CONF_TRANSPORT_TYPE).equals("http")) {
      findSparkConfigKey(conf, SPARK_CONF_HTTP_URL)
          .ifPresent(url -> UrlParser.parseUrl(url).forEach(conf::set));
    }
    findSparkConfigKey(conf, SPARK_CONF_APP_NAME)
        .filter(str -> !str.isEmpty())
        .ifPresent(builder::appName);
    findSparkConfigKey(conf, SPARK_CONF_NAMESPACE).ifPresent(builder::namespace);
    findSparkConfigKey(conf, SPARK_CONF_JOB_NAME).ifPresent(builder::jobName);
    findSparkConfigKey(conf, SPARK_CONF_PARENT_RUN_ID).ifPresent(builder::parentRunId);
    builder.openLineageYaml(extractOpenlineageConfFromSparkConf(conf));
    return builder.build();
  }

  public static OpenLineageYaml extractOpenlineageConfFromSparkConf(SparkConf conf) {
    List<Tuple2<String, String>> olconf =
        Arrays.stream(conf.getAllWithPrefix("spark.openlineage."))
            .filter(e -> e._1.startsWith("transport") || e._1.startsWith("facets"))
            .collect(Collectors.toList());

    JSONObject jsonObject = new JSONObject();
    for (Tuple2<String, String> c : olconf) {
      JSONObject temp = jsonObject;
      String keyPath = c._1;
      String value = c._2;
      Optional<String> propertyPath =
          PROPERTIES_PREFIXES.stream().filter(keyPath::startsWith).findAny();
      List<String> pathKeys =
          propertyPath
              .map(
                  s -> {
                    List<String> path = new ArrayList<>(Arrays.asList(s.split("\\.")));
                    path.add(keyPath.replaceFirst(s, ""));
                    return path;
                  })
              .orElseGet(() -> Arrays.asList(keyPath.split("\\.")));
      List<String> nonLeafs = pathKeys.subList(0, pathKeys.size() - 1);
      String leaf = pathKeys.get(pathKeys.size() - 1);
      for (String node : nonLeafs) {
        if (!temp.has(node)) {
          temp.put(node, new JSONObject());
        }
        temp = temp.getJSONObject(node);
      }
      if (value.contains(DISABLED_FACETS_SEPARATOR)) {
        JSONArray jsonArray = new JSONArray();
        Arrays.stream(value.split(DISABLED_FACETS_SEPARATOR))
            .filter(StringUtils::isNotBlank)
            .forEach(jsonArray::put);
        temp.put(leaf, jsonArray);
      } else {
        temp.put(leaf, value);
      }
    }
    return OpenLineageClientUtils.loadOpenLineageYaml(
        new ByteArrayInputStream(jsonObject.toString().getBytes()));
  }
}

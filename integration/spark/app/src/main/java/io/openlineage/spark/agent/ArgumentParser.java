/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.OpenLineageYaml;
import io.openlineage.client.transports.TransportConfig;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

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

@AllArgsConstructor
@Slf4j
@Getter
@ToString
@Builder
public class ArgumentParser {
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
  @Builder.Default private OpenLineageYaml openLineageYaml = new OpenLineageYaml();
  

  public static OpenLineageYaml extractOpenlineageConfFromSparkConf(SparkConf conf) {
    Tuple2<String, String>[] olconf = conf.getAllWithPrefix("spark.openlineage");
    JSONObject jsonObject = new JSONObject();
    for(Tuple2<String, String> tuple: olconf){
      JSONObject temp = jsonObject;
      String keyString = tuple._1.substring(1, tuple._1.length());
      String[] jpath = keyString.split("\\.");
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
          if(keyString.equals("facets.disabled")) {
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

  public static void parse(ArgumentParserBuilder builder, String clientUrl) {
    URI uri = URI.create(clientUrl);
    String path = uri.getPath();
    String[] elements = path.split("/");
    List<NameValuePair> nameValuePairList = URLEncodedUtils.parse(uri, StandardCharsets.UTF_8);

    builder
        .host(uri.getScheme() + "://" + uri.getAuthority())
        .timeout(getTimeout(nameValuePairList))
        .apiKey(getNamedStringParameter(nameValuePairList, "api_key"))
        .appName(getNamedStringParameter(nameValuePairList, "app_name"))
        .urlParams(getUrlParams(nameValuePairList))
        .consoleMode(false);

    get(elements, "api", 1).ifPresent(builder::version);
    get(elements, "namespaces", 3).ifPresent(builder::namespace);
    get(elements, "jobs", 5).ifPresent(builder::jobName);
    get(elements, "runs", 7).ifPresent(builder::parentRunId);
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

  public static UUID getRandomUuid() {
    return UUID.randomUUID();
  }

  private static Optional<String> getNamedStringParameter(
      List<NameValuePair> nameValuePairList, String name) {
    return Optional.ofNullable(getNamedParameter(nameValuePairList, name))
        .filter(StringUtils::isNoneBlank);
  }

  private static Optional<Double> getTimeout(List<NameValuePair> nameValuePairList) {
    return Optional.ofNullable(
        ArgumentParser.extractTimeout(getNamedParameter(nameValuePairList, "timeout")));
  }

  private static Double extractTimeout(String timeoutString) {
    try {
      if (StringUtils.isNotBlank(timeoutString)) {
        return Double.parseDouble(timeoutString);
      }
    } catch (NumberFormatException e) {
      log.warn("Value of timeout is not parsable");
    }
    return null;
  }

  public String getUrlParam(String urlParamName) {
    String param = null;
    if (urlParams.isPresent()) {
      param = urlParams.get().get(urlParamName);
    }
    return param;
  }

  public String[] getDisabledFacets() {
    return disabledFacets.split(disabledFacetsSeparator);
  }

  private static Optional<Map<String, String>> getUrlParams(List<NameValuePair> nameValuePairList) {
    final Map<String, String> urlParams = new HashMap<String, String>();
    nameValuePairList.stream()
        .filter(pair -> !namedParams.contains(pair.getName()))
        .forEach(pair -> urlParams.put(pair.getName(), pair.getValue()));

    return urlParams.isEmpty() ? Optional.empty() : Optional.ofNullable(urlParams);
  }

  protected static String getNamedParameter(List<NameValuePair> nameValuePairList, String param) {
    for (NameValuePair nameValuePair : nameValuePairList) {
      if (nameValuePair.getName().equalsIgnoreCase(param)) {
        return nameValuePair.getValue();
      }
    }
    return null;
  }

  private static Optional<String> get(String[] elements, String name, int index) {
    boolean check = elements.length > index + 1 && name.equals(elements[index]);
    if (check) {
      return Optional.of(elements[index + 1]);
    } else {
      log.warn("missing " + name + " in " + Arrays.toString(elements) + " at " + index);
      return Optional.empty();
    }
  }
}

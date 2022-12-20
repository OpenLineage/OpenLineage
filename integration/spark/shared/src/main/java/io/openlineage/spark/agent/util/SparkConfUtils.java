/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import scala.Option;

@Slf4j
public class SparkConfUtils {
  private static final String metastoreUriKey = "spark.sql.hive.metastore.uris";
  private static final String metastoreHadoopUriKey = "spark.hadoop.hive.metastore.uris";

  public static Map<String, String> findSparkConfigKeysStartsWith(SparkConf conf, String prefix) {
    return Arrays.stream(conf.getAllWithPrefix(prefix))
        .collect(Collectors.toMap(t -> t._1, t -> t._2));
  }

  public static Optional<String> findSparkConfigKey(SparkConf conf, String name) {
    Option<String> opt = conf.getOption(name);
    if (opt.isDefined()) {
      return Optional.of(opt.get());
    }
    // handling deprecated http properties without 'transport'
    opt = conf.getOption(name.replace("transport.", ""));
    if (opt.isDefined()) {
      return Optional.of(opt.get());
    }
    return Optional.empty();
  }

  public static Optional<Double> findSparkConfigKeyDouble(SparkConf conf, String name) {
    Option<String> opt = conf.getOption(name);
    if (!opt.isDefined()) {
      opt = conf.getOption("spark." + name);
    }
    if (opt.isDefined()) {
      try {
        if (StringUtils.isNotBlank(opt.get())) {
          return Optional.of(Double.parseDouble(opt.get()));
        }
      } catch (NumberFormatException e) {
        log.warn("Value of timeout is not parsable");
      }
    }
    return Optional.empty();
  }

  public static Optional<URI> getMetastoreUri(SparkConf conf) {
    return Optional.ofNullable(
            SparkConfUtils.findSparkConfigKey(conf, metastoreUriKey)
                .orElse(
                    SparkConfUtils.findSparkConfigKey(conf, metastoreHadoopUriKey).orElse(null)))
        .map(
            key -> {
              if (key.contains(",")) {
                return Arrays.stream(key.split(",")).findFirst().get();
              }
              return key;
            })
        .map(
            uri -> {
              try {
                return new URI(uri);
              } catch (URISyntaxException e) {
                return null;
              }
            });
  }

  public static Optional<Map<String, String>> findSparkUrlParams(SparkConf conf, String prefix) {
    Map<String, String> urlParams = new HashMap<String, String>();
    scala.Tuple2<String, String>[] urlConfigs = conf.getAllWithPrefix("spark." + prefix + ".");
    for (scala.Tuple2<String, String> param : urlConfigs) {
      urlParams.put(param._1, param._2);
    }

    return Optional.ofNullable(urlParams);
  }
}

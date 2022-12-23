/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import scala.Option;

@Slf4j
public class SparkConfUtils {
  private static final String metastoreUriKey = "spark.sql.hive.metastore.uris";
  private static final String metastoreHadoopUriKey = "spark.hadoop.hive.metastore.uris";

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
}

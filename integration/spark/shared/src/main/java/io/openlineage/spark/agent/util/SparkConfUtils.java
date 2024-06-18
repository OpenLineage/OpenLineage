/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import scala.Option;

public class SparkConfUtils {
  private static final String metastoreUriKey = "spark.sql.hive.metastore.uris";
  private static final String metastoreHadoopUriKey = "hive.metastore.uris";

  public static Optional<String> findSparkConfigKey(SparkConf conf, String name) {
    Option<String> opt = conf.getOption(name);
    if (opt.isDefined()) {
      return Optional.of(opt.get());
    }
    return Optional.empty();
  }

  public static Optional<String> findHadoopConfigKey(Configuration conf, String name) {
    String opt = conf.get(name);
    return Optional.ofNullable(opt);
  }

  public static Optional<URI> getMetastoreUri(SparkContext context) {
    Optional<String> metastoreUris = findSparkConfigKey(context.getConf(), metastoreUriKey);
    if (!metastoreUris.isPresent()) {
      metastoreUris = findHadoopConfigKey(context.hadoopConfiguration(), metastoreHadoopUriKey);
    }
    return metastoreUris
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

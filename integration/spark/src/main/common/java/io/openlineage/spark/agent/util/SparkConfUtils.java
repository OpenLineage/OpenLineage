package io.openlineage.spark.agent.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.SparkConf;
import scala.Option;

public class SparkConfUtils {
  private static final String metastoreUriKey = "spark.sql.hive.metastore.uris";
  private static final String metastoreHadoopUriKey = "spark.hadoop.hive.metastore.uris";

  public static String findSparkConfigKey(SparkConf conf, String name, String defaultValue) {
    return findSparkConfigKey(conf, name).orElse(defaultValue);
  }

  public static Optional<String> findSparkConfigKey(SparkConf conf, String name) {
    Option<String> opt = conf.getOption(name);
    if (opt.isDefined()) {
      return Optional.of(opt.get());
    }
    opt = conf.getOption("spark." + name);
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

  public static Optional<Map<String, String>> findSparkUrlParams(SparkConf conf, String prefix) {
    Map<String, String> urlParams = new HashMap<String, String>();
    scala.Tuple2<String, String>[] urlConfigs = conf.getAllWithPrefix("spark." + prefix + ".");
    for (scala.Tuple2<String, String> param : urlConfigs) {
      urlParams.put(param._1, param._2);
      System.out.println(param._1);
    }

    return Optional.ofNullable(urlParams);
  }
}

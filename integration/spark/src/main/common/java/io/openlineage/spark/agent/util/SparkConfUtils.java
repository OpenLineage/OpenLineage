package io.openlineage.spark.agent.util;

import java.util.Arrays;
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

  public static Optional<String> getMetastoreKey(SparkConf conf) {
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
            });
  }
}

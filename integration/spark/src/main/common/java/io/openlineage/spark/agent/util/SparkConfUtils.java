package io.openlineage.spark.agent.util;

import java.util.Optional;
import org.apache.spark.SparkConf;

public class SparkConfUtils {
  private static final String metastoreUriKey = "spark.sql.hive.metastore.uris";
  private static final String metastoreHadoopUriKey = "spark.hadoop.hive.metastore.uris";

  public static String findSparkConfigKey(SparkConf conf, String name, String defaultValue) {
    return findSparkConfigKey(conf, name).orElse(defaultValue);
  }

  public static Optional<String> findSparkConfigKey(SparkConf conf, String name) {
    return ScalaConversionUtils.asJavaOptional(
        conf.getOption(name)
            .getOrElse(ScalaConversionUtils.toScalaFn(() -> conf.getOption("spark." + name))));
  }

  public static Optional<String> getMetastoreKey(SparkConf conf) {
    return Optional.ofNullable(
        SparkConfUtils.findSparkConfigKey(conf, metastoreUriKey)
            .orElse(SparkConfUtils.findSparkConfigKey(conf, metastoreHadoopUriKey).orElse(null)));
  }
}

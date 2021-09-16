package io.openlineage.spark.agent.util;

import java.util.Optional;
import org.apache.spark.SparkConf;

public class SparkConfUtils {
  public static String findSparkConfigKey(SparkConf conf, String name, String defaultValue) {
    return findSparkConfigKey(conf, name).orElse(defaultValue);
  }

  public static Optional<String> findSparkConfigKey(SparkConf conf, String name) {
    return ScalaConversionUtils.asJavaOptional(
        conf.getOption(name)
            .getOrElse(ScalaConversionUtils.toScalaFn(() -> conf.getOption("spark." + name))));
  }
}

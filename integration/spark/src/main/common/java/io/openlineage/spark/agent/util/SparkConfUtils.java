package io.openlineage.spark.agent.util;

import java.util.HashMap;
import java.util.Map;
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

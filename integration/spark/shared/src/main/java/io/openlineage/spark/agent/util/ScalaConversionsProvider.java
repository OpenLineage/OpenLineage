package io.openlineage.spark.agent.util;

public final class ScalaConversionsProvider {
  private static final String SCALA_2_12_CONVERSIONS_CLASS =
      "io.openlineage.spark.agent.util.ScalaConversions212";
  private static final String SCALA_2_13_CONVERSIONS_CLASS =
      "io.openlineage.spark.agent.util.ScalaConversions213";

  private ScalaConversionsProvider() {}

  public static ScalaConversions getScalaConversions() {
    try {
      String className =
          ScalaVersionUtil.isScala213()
              ? SCALA_2_13_CONVERSIONS_CLASS
              : SCALA_2_12_CONVERSIONS_CLASS;
      return (ScalaConversions) Class.forName(className).newInstance();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Unable to find ScalaConversions implementation", e);
    } catch (InstantiationException e) {
      throw new RuntimeException("Unable to instantiate ScalaConversions implementation", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Unable to access ScalaConversions implementation", e);
    }
  }
}

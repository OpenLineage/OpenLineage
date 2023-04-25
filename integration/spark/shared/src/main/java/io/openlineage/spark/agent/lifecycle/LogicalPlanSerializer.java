/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonIgnoreType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.ClassIntrospector;
import com.fasterxml.jackson.databind.introspect.ClassIntrospector.MixInResolver;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.reflect.MethodUtils;
import org.apache.spark.Partition;
import org.apache.spark.api.python.PythonRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.SQLExecutionRDD;

/**
 * {@link LogicalPlan} serializer which serialize {@link LogicalPlan} to JSON string. This
 * serializer relies on Jackson's {@link com.fasterxml.jackson.module.scala.DefaultScalaModule},
 * which is scala-version specific. That is why we need to use Jackson library available within
 * Spark and not shaded one included within openlineage-java. This poses some limitations as logical
 * plan serialization relies on mixIns and annotations, however annotations class are relocated and
 * cannot be replaced with dynamic proxy classes. The major limitation of this approach is that we
 * cannot use JsonTypeInfo annotation to enrich jsons with class names within the plan.
 */
@Slf4j
class LogicalPlanSerializer {
  private static final int MAX_SERIALIZED_PLAN_LENGTH =
      50000; // 50K UTF-8 chars should be ~200KB + some extra bytes added during json encoding
  private final Object objectMapper;

  /** relocate plugin rewrites by default all occurences of com.fasterxml.jackson */
  private static final String UNSHADED_JACKSON_PACKAGE = "com.".trim() + "fasterxml.jackson";

  public LogicalPlanSerializer() {
    // we need to use non-relocated Spark's ObjectMapper to serialize logical plan
    objectMapper = getObjectMapper();

    try {
      /**
       * We want to use LogicalPlanMixinResolver to add some extra Jackson annotations on how a plan
       * should be serialized. This can be achieved by running: `objectMapper.setMixInResolver(new
       * LogicalPlanMixinResolver)` However, this won't work as we want to use non-shaded original
       * ObjectMapper and LogicalPlanMixinResolver available in this class will be relocated to
       * other package. The issue can be solved by creating dynamic proxy class which will implement
       * non-shaded MixInResolver and its methods will call shaded LogicalPlanMixinResolver methods.
       * This allows merging together non-shaded and shaded Jackson classes.
       */
      Class clazz =
          Class.forName(
              UNSHADED_JACKSON_PACKAGE + ".databind.introspect.ClassIntrospector$MixInResolver");

      Object resolver =
          Proxy.newProxyInstance(
              clazz.getClassLoader(),
              new Class[] {clazz},
              new InvocationHandler() {
                LogicalPlanMixinResolver resolver = new LogicalPlanMixinResolver();

                @Override
                public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                  return MethodUtils.invokeMethod(resolver, method.getName(), args);
                }
              });

      MethodUtils.invokeMethod(
          objectMapper,
          "registerModule",
          Class.forName(UNSHADED_JACKSON_PACKAGE + ".module.scala.DefaultScalaModule$")
              .getDeclaredField("MODULE$")
              .get(null));

      MethodUtils.invokeMethod(objectMapper, "setMixInResolver", resolver);
    } catch (Exception | Error t) {
      log.warn("Can't register jackson scala module for serializing LogicalPlan", t);
    }
  }

  private Object getObjectMapper() {
    try {
      return Class.forName(UNSHADED_JACKSON_PACKAGE + ".databind.ObjectMapper")
          .getConstructor()
          .newInstance();
    } catch (Exception e) {
      log.warn("Couldn't instantiate ObjectMapper", e);

      // didn't work. Let's get shaded one
      return new ObjectMapper();
    }
  }

  private String writeValueAsString(LogicalPlan x) {
    try {
      return (String) MethodUtils.invokeMethod(objectMapper, "writeValueAsString", x);
    } catch (Exception e) {
      log.warn("Unable to writeValueAsString", e);
      return "";
    }
  }

  private String writeValueAsString(String x) {
    try {
      return (String) MethodUtils.invokeMethod(objectMapper, "writeValueAsString", x);
    } catch (Exception e) {
      log.warn("Unable to writeValueAsString", e);
      return "";
    }
  }

  /**
   * Returns valid JSON string
   *
   * @param x
   * @return
   */
  public String serialize(LogicalPlan x) {
    String serializedPlan = writeValueAsString(x);
    if (serializedPlan.length() > MAX_SERIALIZED_PLAN_LENGTH) {
      // entry is too long, we slice a substring it and send as String field
      serializedPlan = writeValueAsString(serializedPlan.substring(0, MAX_SERIALIZED_PLAN_LENGTH));
    }
    return serializedPlan;
  }

  @JsonIgnoreType
  public static class IgnoredType {}

  /**
   * 'canonicalized' field is ignored due to recursive call and {@link StackOverflowError} 'child'
   * and 'containsChild' fields ignored cause we don't need them for the root node in {@link
   * LogicalPlan} and leaf nodes don't have child nodes in {@link LogicalPlan}
   */
  @JsonIgnoreProperties({
    "child",
    "containsChild",
    "canonicalized",
    "constraints",
    "data",
    "deltaLog"
  })
  @SuppressWarnings("PMD")
  abstract class ChildMixIn {}

  @JsonIgnoreProperties({"sqlConfigs", "sqlConfExecutorSide"})
  public static class SqlConfigMixin {}

  public static class PythonRDDMixin {
    @SuppressWarnings("PMD")
    @JsonIgnore
    private PythonRDDMixin asJavaRDD;
  }

  public static class RDDMixin {
    @SuppressWarnings("PMD")
    @JsonIgnore
    private Partition[] partitions;

    @JsonIgnore
    public Boolean isEmpty() {
      return false;
    }

    @JsonIgnore
    public Partition[] getPartitions() {
      return new Partition[] {};
    }
  }

  static class LogicalPlanMixinResolver implements MixInResolver {
    private static Map<Class, Class> concreteMixin;

    static {
      ImmutableMap.Builder<Class, Class> builder =
          ImmutableMap.<Class, Class>builder()
              .put(PythonRDD.class, PythonRDDMixin.class)
              .put(ClassLoader.class, IgnoredType.class)
              .put(RDD.class, RDDMixin.class)
              .put(SQLExecutionRDD.class, SqlConfigMixin.class)
              .put(FunctionRegistry.class, IgnoredType.class);

      try {
        Class<?> c = ChildMixIn.class.getClassLoader().loadClass("java.lang.Module");
        builder.put(c, IgnoredType.class);
      } catch (Exception e) {
        // ignore
      }

      concreteMixin = builder.build();
    }

    @Override
    public Class<?> findMixInClassFor(Class<?> cls) {
      return concreteMixin.getOrDefault(cls, ChildMixIn.class);
    }

    @Override
    public ClassIntrospector.MixInResolver copy() {
      return this;
    }
  }
}

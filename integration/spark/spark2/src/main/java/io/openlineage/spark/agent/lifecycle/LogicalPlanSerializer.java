/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonIgnoreType;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.ClassIntrospector;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.Partition;
import org.apache.spark.api.python.PythonRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.SQLExecutionRDD;
import org.apache.spark.sql.sources.BaseRelation;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;

/**
 * {@link LogicalPlan} serializer which serialize {@link LogicalPlan} to JSON string. This
 * serializer relies on Jackson's {@link com.fasterxml.jackson.module.scala.DefaultScalaModule},
 * which is scala-version specific. Currently, we depend on the Scala 2.11 version of the jar,
 * making this code incompatible with Spark 3 or other other Spark installations compiled with Scala
 * 2.12 or higher. In such cases, we'll fail to load the Jackson module, but will continue to
 * attempt serialization of the {@link LogicalPlan}. If serialization fails, the exception message
 * and stacktrace will be reported.
 */
@Slf4j
class LogicalPlanSerializer {
  private final ObjectMapper mapper;

  public LogicalPlanSerializer() {
    mapper = new ObjectMapper();
    try {
      mapper.registerModule(
          (Module)
              Class.forName("com.fasterxml.jackson.module.scala.DefaultScalaModule$")
                  .getDeclaredField("MODULE$")
                  .get(null));
    } catch (Throwable t) {
      log.warn("Can't register jackson scala module for serializing LogicalPlan");
    }

    mapper.setMixInResolver(new LogicalPlanMixinResolver());
  }

  /**
   * Returns valid JSON string
   *
   * @param x
   * @return
   */
  public String serialize(LogicalPlan x) {
    try {
      return mapper.writeValueAsString(x);
    } catch (Throwable e) {
      try {
        return mapper.writeValueAsString(
            "Unable to serialize logical plan due to: " + e.getMessage());
      } catch (JsonProcessingException ex) {
        return "\"Unable to serialize error message\"";
      }
    }
  }

  @JsonIgnoreType
  public static class IgnoredType {}

  @JsonTypeInfo(use = Id.CLASS)
  @JsonIdentityInfo(generator = ObjectIdGenerators.IntSequenceGenerator.class, property = "id")
  public static class TypeInfoMixin {}

  /**
   * 'canonicalized' field is ignored due to recursive call and {@link StackOverflowError} 'child'
   * and 'containsChild' fields ignored cause we don't need them for the root node in {@link
   * LogicalPlan} and leaf nodes don't have child nodes in {@link LogicalPlan}
   */
  @JsonIdentityInfo(generator = ObjectIdGenerators.IntSequenceGenerator.class, property = "id")
  @JsonIgnoreProperties({"child", "containsChild", "canonicalized", "constraints"})
  abstract class ChildMixIn {}

  @JsonIgnoreProperties({"sqlConfigs", "sqlConfExecutorSide"})
  public static class SqlConfigMixin {}

  @JsonIdentityInfo(generator = ObjectIdGenerators.IntSequenceGenerator.class, property = "id")
  public static class PythonRDDMixin {
    @JsonIgnore private PythonRDDMixin asJavaRDD;
  }

  @JsonTypeInfo(use = Id.CLASS)
  @JsonIdentityInfo(generator = ObjectIdGenerators.IntSequenceGenerator.class, property = "id")
  public static class RDDMixin {
    @JsonIgnore private Partition[] partitions;

    @JsonIgnore
    public Boolean isEmpty() {
      return false;
    }

    @JsonIgnore
    public Partition[] getPartitions() {
      return null;
    }
  }

  static class PolymorficMixIn extends AbstractPartialFunction<Class, Class> {
    private Class target;
    private Class mixin;

    public PolymorficMixIn(Class target, Class mixin) {
      this.target = target;
      this.mixin = mixin;
    }

    @Override
    public boolean isDefinedAt(Class x) {
      return target.isAssignableFrom(x);
    }

    @Override
    public Class apply(Class clazz) {
      return mixin;
    }
  }

  static class LogicalPlanMixinResolver implements ClassIntrospector.MixInResolver {
    private static Map<Class, Class> concreteMixin;

    static {
      ImmutableMap.Builder<Class, Class> builder =
          ImmutableMap.<Class, Class>builder()
              .put(PythonRDD.class, PythonRDDMixin.class)
              .put(ClassLoader.class, IgnoredType.class)
              .put(RDD.class, RDDMixin.class)
              .put(SQLExecutionRDD.class, SqlConfigMixin.class);
      try {
        Class<?> c = PolymorficMixIn.class.getClassLoader().loadClass("java.lang.Module");
        builder.put(c, IgnoredType.class);
      } catch (Exception e) {
        // ignore
      }
      concreteMixin = builder.build();
    }

    private static List<PartialFunction<Class, Class>> polymorficMixIn =
        ImmutableList.of(
            new PolymorficMixIn(LogicalPlan.class, TypeInfoMixin.class),
            new PolymorficMixIn(BaseRelation.class, TypeInfoMixin.class));

    @Override
    public Class<?> findMixInClassFor(Class<?> cls) {
      Supplier<Class> defaultMixin =
          () ->
              polymorficMixIn.stream()
                  .filter(fun -> fun.isDefinedAt(cls))
                  .findFirst()
                  .map(f -> f.apply(cls))
                  .orElse(ChildMixIn.class);
      return concreteMixin.getOrDefault(cls, defaultMixin.get());
    }

    @Override
    public ClassIntrospector.MixInResolver copy() {
      return this;
    }
  }
}

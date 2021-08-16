package openlineage.spark.agent.lifecycle.plan;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonIgnoreType;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.ClassIntrospector;
import com.fasterxml.jackson.module.scala.DefaultScalaModule$;
import org.apache.spark.Partition;
import org.apache.spark.api.python.PythonRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.trees.TreeNode;
import org.apache.spark.sql.sources.BaseRelation;

public class LogicalPlanSerializer {
  private final ObjectMapper mapper;
  private static final LogicalPlanSerializer instance = new LogicalPlanSerializer();

  public static LogicalPlanSerializer getInstance() {
    return instance;
  }

  private LogicalPlanSerializer() {
    mapper = new ObjectMapper();
    mapper.registerModule(DefaultScalaModule$.MODULE$);

    mapper.addMixIn(PythonRDD.class, PythonRDDMixin.class);
    mapper.addMixIn(RDD.class, RDDMixin.class);
    mapper.addMixIn(TreeNode.class, TypeInfoMixin.class);
    mapper.addMixIn(ClassLoader.class, IgnoredType.class);
    mapper.addMixIn(BaseRelation.class, TypeInfoMixin.class);
    mapper.addMixIn(BaseRelation.class, TypeInfoMixin.class);
    mapper.setMixInResolver(
        new ClassIntrospector.MixInResolver() {
          @Override
          public Class<?> findMixInClassFor(Class<?> cls) {
            return ChildMixIn.class;
          }

          @Override
          public ClassIntrospector.MixInResolver copy() {
            return this;
          }
        });
    try {
      Class<?> c = getClass().getClassLoader().loadClass("java.lang.Module");
      mapper.addMixIn(c, IgnoredType.class);
    } catch (Exception e) {
      // ignore
    }
  }

  public String serialize(LogicalPlan x) {
    try {
      return mapper.writeValueAsString(x);
    } catch (JsonProcessingException e) {
      return "Unable to serialize {}: " + e.getMessage();
    }
  }

  @JsonIgnoreType
  public static class IgnoredType {}

  @JsonTypeInfo(use = Id.CLASS)
  public static class TypeInfoMixin {}

  @JsonIgnoreProperties({"child", "containsChild", "canonicalized"})
  abstract class ChildMixIn {}

  public static class PythonRDDMixin {
    @JsonIgnore private PythonRDDMixin asJavaRDD;
  }

  @JsonTypeInfo(use = Id.CLASS)
  @JsonIdentityInfo(generator = ObjectIdGenerators.IntSequenceGenerator.class, property = "id")
  public static class RDDMixin {
    @JsonIgnore private Partition[] partitions;

    @JsonIgnore
    public Partition[] getPartitions() {
      return null;
    }
  }
}

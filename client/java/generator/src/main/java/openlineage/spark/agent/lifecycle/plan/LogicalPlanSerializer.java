package openlineage.spark.agent.lifecycle.plan;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreType;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.Partition;
import org.apache.spark.api.python.PythonRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.trees.TreeNode;
import org.apache.spark.sql.sources.BaseRelation;

public class LogicalPlanSerializer {
  private static final ObjectMapper mapper;

  static {
    mapper = new ObjectMapper();
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    // mapper.registerModule(DefaultScalaModule$.MODULE$);

    mapper.addMixIn(Expression.class, ExpressionMixin.class);
    mapper.addMixIn(PythonRDD.class, PythonRDDMixin.class);
    mapper.addMixIn(RDD.class, RDDMixin.class);
    mapper.addMixIn(TreeNode.class, TypeInfoMixin.class);
    mapper.addMixIn(ClassLoader.class, IgnoredType.class);
    mapper.addMixIn(BaseRelation.class, TypeInfoMixin.class);
    try {
      Class<?> c = LogicalPlanSerializer.class.getClassLoader().loadClass("java.lang.Module");
      mapper.addMixIn(c, IgnoredType.class);
    } catch (Exception e) {
      // ignore
    }
  }

  public String serialize(LogicalPlan plan) {
    try {
      return mapper.writeValueAsString(plan);
    } catch (JsonProcessingException e) {
      return "Unable to serialize {}: " + e.getMessage();
    }
  }

  @JsonIgnoreType
  public static class IgnoredType {}

  @JsonTypeInfo(include = As.PROPERTY, use = Id.CLASS)
  public static class TypeInfoMixin {}

  public static class ExpressionMixin {
    @JsonIgnore private Expression canonicalized;
  }

  public static class PythonRDDMixin {
    @JsonIgnore private PythonRDDMixin asJavaRDD;
  }

  @JsonTypeInfo(include = As.PROPERTY, use = Id.CLASS)
  @JsonIdentityInfo(generator = ObjectIdGenerators.IntSequenceGenerator.class, property = "id")
  public static class RDDMixin {
    @JsonIgnore private Partition[] partitions;

    @JsonIgnore
    public Partition[] getPartitions() {
      return null;
    }
  }
}

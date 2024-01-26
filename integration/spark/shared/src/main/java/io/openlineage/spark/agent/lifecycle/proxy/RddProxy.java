package io.openlineage.spark.agent.lifecycle.proxy;

import io.openlineage.spark.agent.util.ScalaVersionUtil;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Objects;
import org.apache.spark.Dependency;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.rdd.HadoopRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.ShuffledRowRDD;
import org.apache.spark.sql.execution.datasources.FileScanRDD;
import scala.collection.JavaConverters;

public final class RddProxy {
  private final RDD<?> delegate;
  private final MethodHandle dependenciesMethodHandle;

  public RddProxy(RDD<?> delegate) {
    this.delegate = Objects.requireNonNull(delegate);
    this.dependenciesMethodHandle = setupDependenciesMethodHandle();
  }

  private MethodHandle setupDependenciesMethodHandle() {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      boolean isScala213 = ScalaVersionUtil.isScala213();
      Class<?> seqClass =
          isScala213 ? scala.collection.immutable.Seq.class : scala.collection.Seq.class;
      return lookup
          .findVirtual(RDD.class, "dependencies", MethodType.methodType(seqClass))
          .bindTo(delegate);
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new RuntimeException("Failed to setup method handle for RDD#dependencies", e);
    }
  }

  public List<Dependency<?>> dependencies() {
    try {
      Object dependencies = dependenciesMethodHandle.invoke();
      return convert(dependencies);
    } catch (Throwable throwable) {
      throw new RuntimeException("Failed to invoke dependencies on RDD delegate", throwable);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private List<Dependency<?>> convert(Object dependencies) {
    if (dependencies instanceof scala.collection.immutable.Seq) {
      return JavaConverters.seqAsJavaList((scala.collection.immutable.Seq) dependencies);
    } else if (dependencies instanceof scala.collection.Seq) {
      return JavaConverters.seqAsJavaList((scala.collection.Seq) dependencies);
    } else {
      throw new RuntimeException(
          "Unexpected type for RDD dependencies: " + dependencies.getClass());
    }
  }

  public boolean isShuffledRowRdd() {
    return delegate instanceof ShuffledRowRDD;
  }

  public ShuffleDependency<Object, InternalRow, InternalRow> dependency() {
    return ((ShuffledRowRDD) delegate).dependency();
  }

  public RDD<?> proxiedRDD() {
    return delegate;
  }

  public boolean isFileScanRdd() {
    return delegate instanceof FileScanRDD;
  }

  public boolean isHadoopRdd() {
    return delegate instanceof HadoopRDD;
  }
}

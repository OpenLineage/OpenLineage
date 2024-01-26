package io.openlineage.spark.agent.lifecycle.proxy;

import io.openlineage.spark.agent.util.ScalaVersionUtil;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.apache.spark.scheduler.SparkListenerJobStart;
import scala.collection.JavaConverters;

public final class SparkListenerJobStartProxy {
  private final SparkListenerJobStart jobStart;
  private final MethodHandle stageIdsMethodHandle;

  public SparkListenerJobStartProxy(SparkListenerJobStart jobStart) {
    this.jobStart = Objects.requireNonNull(jobStart);
    this.stageIdsMethodHandle = setupStageIdsMethodHandle();
  }

  // This method sucks. It leaks the delegate, but it would be too much effort
  // to change the EventFilterUtils#isDisabled method to use the proxy.
  public SparkListenerJobStart proxiedEvent() {
    return jobStart;
  }

  private MethodHandle setupStageIdsMethodHandle() {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      boolean isScala213 = ScalaVersionUtil.isScala213();
      Class<?> seqClass =
          isScala213 ? scala.collection.immutable.Seq.class : scala.collection.Seq.class;
      return lookup
          .findVirtual(SparkListenerJobStart.class, "stageIds", MethodType.methodType(seqClass))
          .bindTo(jobStart);
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new RuntimeException(
          "Failed to setup method handle for SparkListenerJobStart#stageIds", e);
    }
  }

  public Integer jobId() {
    return jobStart.jobId();
  }

  public List<Integer> stageIds() {
    try {
      Object stageIds = stageIdsMethodHandle.invoke();
      return convert(stageIds);
    } catch (Throwable throwable) {
      throw new RuntimeException(
          "Failed to invoke stageIds on SparkListenerJobStart delegate", throwable);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private List<Integer> convert(Object stageIds) {
    if (stageIds instanceof scala.collection.immutable.Seq) {
      return JavaConverters.seqAsJavaList((scala.collection.immutable.Seq) stageIds);
    } else if (stageIds instanceof scala.collection.Seq) {
      return JavaConverters.seqAsJavaList((scala.collection.Seq) stageIds);
    } else {
      throw new RuntimeException(
          "Unexpected type for SparkListenerJobStart stageIds: " + stageIds.getClass());
    }
  }

  public Properties properties() {
    return jobStart.properties();
  }

  public long time() {
    return jobStart.time();
  }

  public String name() {
    return jobStart.properties().getProperty("spark.job.description");
  }
}

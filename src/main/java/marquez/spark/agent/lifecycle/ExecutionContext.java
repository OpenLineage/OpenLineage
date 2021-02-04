package marquez.spark.agent.lifecycle;

import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;

public interface ExecutionContext {

  void setActiveJob(ActiveJob activeJob);

  void start(SparkListenerJobStart jobStart);

  void end(SparkListenerJobEnd jobEnd);
}

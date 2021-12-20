package io.openlineage.spark.agent;

import io.openlineage.spark.agent.lifecycle.ContextFactory;
import io.openlineage.spark.agent.transformers.BigQueryRelationTransformer;
import io.openlineage.spark.agent.transformers.PairRDDFunctionsTransformer;
import io.openlineage.spark.agent.transformers.SparkContextTransformer;
import java.lang.instrument.Instrumentation;
import java.net.URISyntaxException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SparkAgent {
  /** Entry point for -javaagent, pre application start */
  @SuppressWarnings("unused")
  public static void premain(String agentArgs, Instrumentation inst) {
    try {
      premain(
          agentArgs, inst, new ContextFactory(new EventEmitter(ArgumentParser.parse(agentArgs))));
    } catch (URISyntaxException e) {
      log.error("Could not find openlineage client url", e);
    }
  }

  public static void premain(
      String agentArgs, Instrumentation inst, ContextFactory contextFactory) {
    log.info("SparkAgent.premain ");
    OpenLineageSparkListener.init(contextFactory);
    instrument(inst);
    addShutDownHook();
  }

  /** Entry point when attaching after application start */
  @SuppressWarnings("unused")
  public static void agentmain(String agentArgs, Instrumentation inst) {
    premain(agentArgs, inst);
  }

  public static void instrument(Instrumentation inst) {
    inst.addTransformer(new SparkContextTransformer());
    inst.addTransformer(new PairRDDFunctionsTransformer());
    inst.addTransformer(new BigQueryRelationTransformer());
  }

  private static void addShutDownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(OpenLineageSparkListener::close));
  }
}

package marquez.spark.agent;

import java.lang.instrument.Instrumentation;
import java.net.URISyntaxException;
import lombok.extern.slf4j.Slf4j;
import marquez.spark.agent.lifecycle.ContextFactory;
import marquez.spark.agent.transformers.BigQueryRelationTransformer;
import marquez.spark.agent.transformers.PairRDDFunctionsTransformer;
import marquez.spark.agent.transformers.SparkContextTransformer;

@Slf4j
public class MarquezAgent {
  /** Entry point for -javaagent, pre application start */
  @SuppressWarnings("unused")
  public static void premain(String agentArgs, Instrumentation inst) {
    try {
      premain(
          agentArgs, inst, new ContextFactory(new MarquezContext(ArgumentParser.parse(agentArgs))));
    } catch (URISyntaxException e) {
      log.error("Could not find marquez client url", e);
    }
  }

  public static void premain(
      String agentArgs, Instrumentation inst, ContextFactory contextFactory) {
    log.info("MarquezAgent.premain ");
    SparkListener.init(contextFactory);
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
    Runtime.getRuntime().addShutdownHook(new Thread(SparkListener::close));
  }
}

package marquez.spark.agent.lifecycle.plan;

/**
 * DatasetSource is an interface that allows instrumenting an existing Relation or Datasource class
 * with the ability to report its OpenLineage namespace and name. Relations that intend to
 * participate in the OpenLineage reporting can implement this interface directly and they will be
 * found in the Spark {@link org.apache.spark.sql.catalyst.plans.logical.LogicalPlan} during plan
 * execution. Otherwise, {@link java.lang.instrument.ClassFileTransformer}s can be instrumented to
 * dynamically rewrite classes to implement this interface with logic to return the necessary data.
 * In that case, the instrumented class will still be found in the {@link
 * org.apache.spark.sql.catalyst.plans.logical.LogicalPlan} during execution and the datasource
 * details will be included in the lineage.
 */
public interface DatasetSource {

  /**
   * @return the {@link marquez.spark.agent.client.LineageEvent.Dataset#namespace} that will be
   *     reported to the OpenLineage service
   */
  String namespace();

  /**
   * @return the {@link marquez.spark.agent.client.LineageEvent.Dataset#name} that will be reported
   *     to the OpenLineage service
   */
  String name();
}

package marquez.spark.agent.facets;

import lombok.EqualsAndHashCode;
import lombok.Value;
import marquez.spark.agent.client.LineageEvent;

/**
 * Facet with statistics for an output dataset, including the number of rows and the size of the
 * output in bytes.
 */
@EqualsAndHashCode(callSuper = true)
@Value
public class OutputStatisticsFacet extends LineageEvent.BaseFacet {
  long rowCount;
  long size;
}

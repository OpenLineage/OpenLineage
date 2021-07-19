package openlineage.spark.agent.facets;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import lombok.Builder;
import lombok.NonNull;
import openlineage.spark.agent.client.LineageEvent.BaseFacet;
import openlineage.spark.agent.client.OpenLineageClient;

public class ErrorFacet extends BaseFacet {
  private final Exception exception;

  @Builder
  public ErrorFacet(@NonNull Exception exception) {
    super(
        URI.create(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI),
        URI.create(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI + "/facets/spark-2.4/v1/error-facet"));
    this.exception = exception;
  }

  public String getMessage() {
    return exception.getMessage();
  }

  public String getStackTrace() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    exception.printStackTrace(pw);
    return sw.toString();
  }
}

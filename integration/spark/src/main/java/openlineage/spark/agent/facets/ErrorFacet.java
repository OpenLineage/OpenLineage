package openlineage.spark.agent.facets;

import io.openlineage.client.OpenLineage;
import java.io.PrintWriter;
import java.io.StringWriter;
import lombok.Builder;
import lombok.NonNull;
import openlineage.spark.agent.client.OpenLineageClient;

public class ErrorFacet extends OpenLineage.CustomFacet {
  private final Exception exception;

  @Builder
  public ErrorFacet(@NonNull Exception exception) {
    super(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI);
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

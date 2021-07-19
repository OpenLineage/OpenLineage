package marquez.spark.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.net.URISyntaxException;
import org.junit.jupiter.api.Test;

public class MarquezContextTest {

  @Test
  public void testLineageUri() throws URISyntaxException {
    MarquezContext ctx =
        new MarquezContext(
            ArgumentParser.parse(
                "https://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/"
                    + "ea445b5c-22eb-457a-8007-01c7c52b6e54?api_key=abc"));
    assertEquals(URI.create("https://localhost:5000/api/v1/lineage"), ctx.getLineageURI());
  }
}

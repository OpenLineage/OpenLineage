package marquez.spark.agent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ArgumentParserTest {
  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    List<Object[]> pass = new ArrayList<>();
    pass.add(
        new Object[] {
          "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/ea445b5c-22eb-457a-8007-01c7c52b6e54?api_key=abc",
          "http://localhost:5000",
          "v1",
          "ns_name",
          "job_name",
          "ea445b5c-22eb-457a-8007-01c7c52b6e54",
          false,
          Optional.of("abc")
        });
    pass.add(
        new Object[] {
          "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/ea445b5c-22eb-457a-8007-01c7c52b6e54",
          "http://localhost:5000",
          "v1",
          "ns_name",
          "job_name",
          "ea445b5c-22eb-457a-8007-01c7c52b6e54",
          false,
          Optional.empty()
        });
    pass.add(
        new Object[] {
          "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/ea445b5c-22eb-457a-8007-01c7c52b6e54?api_key=",
          "http://localhost:5000",
          "v1",
          "ns_name",
          "job_name",
          "ea445b5c-22eb-457a-8007-01c7c52b6e54",
          false,
          Optional.empty()
        });
    pass.add(
        new Object[] {
          "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name?api_key=",
          "http://localhost:5000",
          "v1",
          "ns_name",
          "job_name",
          null,
          true,
          Optional.empty()
        });
    return pass;
  }

  @Parameter(value = 0)
  public String input;

  @Parameter(value = 1)
  public String host;

  @Parameter(value = 2)
  public String version;

  @Parameter(value = 3)
  public String namespace;

  @Parameter(value = 4)
  public String jobName;

  @Parameter(value = 5)
  public String runId;

  @Parameter(value = 6)
  public boolean randomUuid;

  @Parameter(value = 7)
  public Optional<String> apiKey;

  @Test
  public void testArgument() {
    ArgumentParser parser = ArgumentParser.parse(input);
    assertEquals(host, parser.getHost());
    assertEquals(version, parser.getVersion());
    assertEquals(namespace, parser.getNamespace());
    assertEquals(jobName, parser.getJobName());
    if (randomUuid) {
      getUuidOrFail(parser.getRunId());
    } else {
      assertEquals(runId, parser.getRunId());
    }
    assertEquals(apiKey, parser.getApiKey());
  }

  private void getUuidOrFail(String uuid) {
    try {
      UUID.fromString(uuid);
    } catch (Exception e) {
      fail("Run id is not a uuid");
    }
  }
}

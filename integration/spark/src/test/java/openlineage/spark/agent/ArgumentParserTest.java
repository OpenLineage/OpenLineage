package openlineage.spark.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ArgumentParserTest {
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

  @ParameterizedTest
  @MethodSource("data")
  public void testArgument(
      String input,
      String host,
      String version,
      String namespace,
      String jobName,
      String runId,
      boolean defaultRunId,
      Optional<String> apiKey) {
    ArgumentParser parser = ArgumentParser.parse(input);
    assertEquals(host, parser.getHost());
    assertEquals(version, parser.getVersion());
    assertEquals(namespace, parser.getNamespace());
    assertEquals(jobName, parser.getJobName());
    if (defaultRunId) {
      assertEquals("", parser.getRunId());
    } else {
      assertEquals(runId, parser.getRunId());
    }
    assertEquals(apiKey, parser.getApiKey());
  }
}

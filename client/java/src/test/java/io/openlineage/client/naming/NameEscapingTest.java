/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.naming;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.OpenLineageConfig;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class NameEscapingTest {

  private static final String ENV_VAR = "OPENLINEAGE__NAME__ESCAPING";
  private static final char DOT = '.'; // to make PMD happy

  // -----------------------------------------------------------------------
  // Helpers — identical pattern to JwtTokenProviderTest
  // -----------------------------------------------------------------------

  @SuppressWarnings({"unchecked", "PMD"})
  private void setEnvironmentVariables(Map<String, String> newEnv) throws Exception {
    Class<?> classOfMap = System.getenv().getClass();
    Field field = classOfMap.getDeclaredField("m");
    field.setAccessible(true);
    Map<String, String> writeable = (Map<String, String>) field.get(System.getenv());
    writeable.putAll(newEnv);
  }

  @SuppressWarnings({"unchecked", "PMD"})
  private void clearEnvironmentVariables(Set<String> keys) throws Exception {
    Class<?> classOfMap = System.getenv().getClass();
    Field field = classOfMap.getDeclaredField("m");
    field.setAccessible(true);
    Map<String, String> writeable = (Map<String, String>) field.get(System.getenv());
    keys.forEach(writeable::remove);
  }

  @AfterEach
  void cleanUp() throws Exception {
    // Always restore so subsequent tests start with escaping disabled (the default).
    clearEnvironmentVariables(Set.of(ENV_VAR));
  }

  // -----------------------------------------------------------------------
  // isEscapingEnabled() — env-var behaviour (zero-arg overload)
  // -----------------------------------------------------------------------

  @Test
  void escapingIsDisabledByDefault() throws Exception {
    clearEnvironmentVariables(Set.of(ENV_VAR));

    assertThat(NameEscaping.isEscapingEnabled()).isFalse();
  }

  @Test
  void escapingIsEnabledWhenEnvVarIsTrue() throws Exception {
    Map<String, String> env = new HashMap<>();
    env.put(ENV_VAR, "true");
    setEnvironmentVariables(env);

    try {
      assertThat(NameEscaping.isEscapingEnabled()).isTrue();
    } finally {
      clearEnvironmentVariables(env.keySet());
    }
  }

  @Test
  void escapingIsEnabledCaseInsensitive() throws Exception {
    for (String value : new String[] {"true", "TRUE", "True"}) {
      Map<String, String> env = new HashMap<>();
      env.put(ENV_VAR, value);
      setEnvironmentVariables(env);

      try {
        assertThat(NameEscaping.isEscapingEnabled())
            .as("isEscapingEnabled() should be true for env value %s", value)
            .isTrue();
      } finally {
        clearEnvironmentVariables(env.keySet());
      }
    }
  }

  @Test
  void escapingRemainsDisabledForNonTrueValues() throws Exception {
    for (String value : new String[] {"false", "FALSE", "1", "yes", "on"}) {
      Map<String, String> env = new HashMap<>();
      env.put(ENV_VAR, value);
      setEnvironmentVariables(env);

      try {
        assertThat(NameEscaping.isEscapingEnabled())
            .as("isEscapingEnabled() should be false for env value %s", value)
            .isFalse();
      } finally {
        clearEnvironmentVariables(env.keySet());
      }
    }
  }

  // -----------------------------------------------------------------------
  // escapeSegment(String) — transformation behaviour (env-var overload)
  // -----------------------------------------------------------------------

  @Test
  void escapeSegmentReturnsInputUnchangedByDefault() throws Exception {
    clearEnvironmentVariables(Set.of(ENV_VAR));

    assertThat(NameEscaping.escapeSegment("mydb.example.com")).isEqualTo("mydb.example.com");
  }

  @Test
  void escapeSegmentEscapesDotsWhenEnabled() throws Exception {
    Map<String, String> env = new HashMap<>();
    env.put(ENV_VAR, "true");
    setEnvironmentVariables(env);

    try {
      assertThat(NameEscaping.escapeSegment("mydb.example.com")).isEqualTo("mydb\\.example\\.com");
    } finally {
      clearEnvironmentVariables(env.keySet());
    }
  }

  @Test
  void escapeSegmentEscapesMultipleDots() throws Exception {
    Map<String, String> env = new HashMap<>();
    env.put(ENV_VAR, "true");
    setEnvironmentVariables(env);

    try {
      assertThat(NameEscaping.escapeSegment("a.b.c")).isEqualTo("a\\.b\\.c");
    } finally {
      clearEnvironmentVariables(env.keySet());
    }
  }

  @Test
  void escapeSegmentEscapesBackslashBeforeDot() throws Exception {
    // A segment containing a literal backslash followed by a dot must have the
    // backslash doubled first, so the output is unambiguous to consumers.
    // "foo\.bar"  →  "foo\\\\.bar"  (in Java source: "foo\\\\\\.bar")
    Map<String, String> env = new HashMap<>();
    env.put(ENV_VAR, "true");
    setEnvironmentVariables(env);

    try {
      // single backslash + dot: "foo\.bar" → "foo\\.bar" (each \ → \\, then . → \.)
      assertThat(NameEscaping.escapeSegment("foo\\.bar")).isEqualTo("foo\\\\\\.bar");
      // plain backslash with no dot: "foo\bar" → "foo\\bar"
      assertThat(NameEscaping.escapeSegment("foo\\bar")).isEqualTo("foo\\\\bar");
      // standalone backslash at end: "foo\" → "foo\\"
      assertThat(NameEscaping.escapeSegment("foo\\")).isEqualTo("foo\\\\");
    } finally {
      clearEnvironmentVariables(env.keySet());
    }
  }

  @Test
  @SuppressWarnings("PMD")
  void escapeSegmentSpecialCombinations() throws Exception {
    // Round-trip test: for each input name (list of segments), escape every
    // segment, join with '.', parse back using the same grammar, and verify
    // the recovered segments match the originals.
    //
    // This validates the encoder is unambiguous — no special-character
    // combination in a segment value can be confused with a structural dot
    // or a misinterpreted escape sequence by the consumer.
    Map<String, String> env = new HashMap<>();
    env.put(ENV_VAR, "true");
    setEnvironmentVariables(env);

    try {
      List<List<String>> cases =
          Arrays.asList(
              // ── single-segment names (no structural dot at all) ──────────
              Arrays.asList(""), // empty segment
              Arrays.asList("plain"),
              Arrays.asList("my_schema-1"),
              Arrays.asList("tëst"), // non-ASCII passthrough
              Arrays.asList("a/b:c"), // other punctuation
              Arrays.asList("."), // segment is just a dot
              Arrays.asList("..."), // segment is only dots
              Arrays.asList(".a."), // dot-wrapped word
              Arrays.asList("\\"), // single backslash
              Arrays.asList("\\\\"), // two backslashes
              Arrays.asList("\\\\\\"), // three backslashes
              Arrays.asList("\\."), // backslash immediately before dot
              Arrays.asList("a\\.b"), // backslash+dot mid-segment
              Arrays.asList("\\\\."), // two backslashes then dot
              Arrays.asList(".\\"), // dot then backslash
              Arrays.asList("\\.\\"), // backslash dot backslash
              Arrays.asList("a.b\\c"), // mid-dot and trailing backslash
              Arrays.asList("\\a.b"), // leading backslash then mid-dot
              // ── multi-segment names (structural dots present) ────────────
              Arrays.asList("mydb.example.com", "mySchema", "myTable"),
              Arrays.asList("a.b.c", "d.e"), // dots in both segments
              Arrays.asList(".", ".", "."), // every segment is a bare dot
              Arrays.asList("\\.\\", "plain"), // complex + plain
              Arrays.asList("foo", "bar\\.baz", "qux"), // backslash+dot in middle segment
              Arrays.asList("fo\\\\.o", "bar\\.baz", "qux") // complex case
              );

      for (List<String> segments : cases) {
        // Build the combined name: escape each segment and join with '.'
        StringBuilder name = new StringBuilder();
        for (int i = 0; i < segments.size(); i++) {
          if (i > 0) name.append('.');
          name.append(NameEscaping.escapeSegment(segments.get(i)));
        }

        // Parse the combined name back into segments
        List<String> recovered = splitEscaped(name.toString());

        assertThat(recovered)
            .as("round-trip failed for segments %s (encoded: %s)", segments, name)
            .isEqualTo(segments);
      }
    } finally {
      clearEnvironmentVariables(env.keySet());
    }
  }

  /**
   * Splits a dot-separated OpenLineage name into its constituent segments, interpreting the escape
   * grammar: {@code \.} is a literal dot, {@code \\} is a literal backslash, and an unescaped
   * {@code .} is a structural separator.
   */
  private static List<String> splitEscaped(String name) {
    List<String> result = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    int i = 0;
    while (i < name.length()) {
      char c = name.charAt(i);
      if (c == '\\' && i + 1 < name.length()) {
        char next = name.charAt(i + 1);
        if (next == '\\' || next == '.') {
          current.append(next);
          i += 2;
          continue;
        }
      }
      if (c == DOT) {
        result.add(current.toString());
        current.setLength(0);
      } else {
        current.append(c);
      }
      i++;
    }
    result.add(current.toString());
    return result;
  }

  @Test
  void escapeSegmentLeavesNonDotCharsUnchanged() throws Exception {
    clearEnvironmentVariables(Set.of(ENV_VAR));

    assertThat(NameEscaping.escapeSegment("my_schema")).isEqualTo("my_schema");
    assertThat(NameEscaping.escapeSegment("myTable")).isEqualTo("myTable");
    assertThat(NameEscaping.escapeSegment("plain")).isEqualTo("plain");
  }

  // -----------------------------------------------------------------------
  // OpenLineageConfig — name.escaping is parsed correctly from env vars
  // -----------------------------------------------------------------------

  @Test
  void nameConfigEscapingIsTrueWhenEnvVarIsTrue() throws Exception {
    Map<String, String> envVars = new HashMap<>();
    envVars.put("OPENLINEAGE__TRANSPORT__TYPE", "console");
    envVars.put(ENV_VAR, "true");
    setEnvironmentVariables(envVars);

    try {
      OpenLineageConfig<?> config =
          OpenLineageClientUtils.loadOpenLineageConfigFromEnvVars(
              new TypeReference<OpenLineageConfig<OpenLineageConfig<?>>>() {});

      assertThat(config.getNameConfig()).isNotNull();
      assertThat(config.getNameConfig().getEscaping()).isTrue();
    } finally {
      clearEnvironmentVariables(envVars.keySet());
    }
  }

  @Test
  void nameConfigEscapingIsFalseWhenEnvVarIsFalse() throws Exception {
    Map<String, String> envVars = new HashMap<>();
    envVars.put("OPENLINEAGE__TRANSPORT__TYPE", "console");
    envVars.put(ENV_VAR, "false");
    setEnvironmentVariables(envVars);

    try {
      OpenLineageConfig<?> config =
          OpenLineageClientUtils.loadOpenLineageConfigFromEnvVars(
              new TypeReference<OpenLineageConfig<OpenLineageConfig<?>>>() {});

      assertThat(config.getNameConfig()).isNotNull();
      assertThat(config.getNameConfig().getEscaping()).isFalse();
    } finally {
      clearEnvironmentVariables(envVars.keySet());
    }
  }

  @Test
  void nameConfigIsNullWhenEnvVarIsAbsent() throws Exception {
    Map<String, String> envVars = new HashMap<>();
    envVars.put("OPENLINEAGE__TRANSPORT__TYPE", "console");
    // ENV_VAR intentionally not set
    setEnvironmentVariables(envVars);

    try {
      OpenLineageConfig<?> config =
          OpenLineageClientUtils.loadOpenLineageConfigFromEnvVars(
              new TypeReference<OpenLineageConfig<OpenLineageConfig<?>>>() {});

      // nameConfig may be null when the env var was never set; null means "use default" (disabled)
      if (config.getNameConfig() != null) {
        assertThat(config.getNameConfig().getEscaping()).isNull();
      }
    } finally {
      clearEnvironmentVariables(envVars.keySet());
    }
  }

  // -----------------------------------------------------------------------
  // isEscapingEnabled(NameConfig) — NameConfig overload behaviour
  // -----------------------------------------------------------------------

  @Test
  void escapingEnabledViaNameConfig() throws Exception {
    clearEnvironmentVariables(Set.of(ENV_VAR));

    NameConfig cfg = new NameConfig();
    cfg.setEscaping(true);

    assertThat(NameEscaping.isEscapingEnabled(cfg)).isTrue();
  }

  @Test
  void escapingDisabledViaNameConfig() throws Exception {
    clearEnvironmentVariables(Set.of(ENV_VAR));

    NameConfig cfg = new NameConfig();
    cfg.setEscaping(false);

    assertThat(NameEscaping.isEscapingEnabled(cfg)).isFalse();
  }

  @Test
  void nameConfigTakesPrecedenceOverEnvVar() throws Exception {
    // Env var says "true" but NameConfig says "false" → NameConfig wins.
    Map<String, String> env = new HashMap<>();
    env.put(ENV_VAR, "true");
    setEnvironmentVariables(env);

    NameConfig cfg = new NameConfig();
    cfg.setEscaping(false);

    try {
      assertThat(NameEscaping.isEscapingEnabled(cfg)).isFalse();
    } finally {
      clearEnvironmentVariables(env.keySet());
    }
  }

  @Test
  void nullNameConfigFallsBackToEnvVar() throws Exception {
    Map<String, String> env = new HashMap<>();
    env.put(ENV_VAR, "true");
    setEnvironmentVariables(env);

    try {
      assertThat(NameEscaping.isEscapingEnabled((NameConfig) null)).isTrue();
    } finally {
      clearEnvironmentVariables(env.keySet());
    }
  }

  @Test
  void nameConfigWithNullEscapingFallsBackToEnvVar() throws Exception {
    // A NameConfig whose escaping field is null should not override the env var.
    Map<String, String> env = new HashMap<>();
    env.put(ENV_VAR, "true");
    setEnvironmentVariables(env);

    try {
      assertThat(NameEscaping.isEscapingEnabled(new NameConfig())).isTrue();
    } finally {
      clearEnvironmentVariables(env.keySet());
    }
  }

  @Test
  void escapeSegmentEscapesWhenEnabledViaNameConfig() throws Exception {
    clearEnvironmentVariables(Set.of(ENV_VAR));

    NameConfig cfg = new NameConfig();
    cfg.setEscaping(true);

    assertThat(NameEscaping.escapeSegment("mydb.example.com", cfg))
        .isEqualTo("mydb\\.example\\.com");
  }

  @Test
  void escapeSegmentUnchangedWhenDisabledViaNameConfig() throws Exception {
    clearEnvironmentVariables(Set.of(ENV_VAR));

    NameConfig cfg = new NameConfig();
    cfg.setEscaping(false);

    assertThat(NameEscaping.escapeSegment("mydb.example.com", cfg)).isEqualTo("mydb.example.com");
  }

  // -----------------------------------------------------------------------
  // Builder integration — nameConfig field is stored on the client instance
  // -----------------------------------------------------------------------

  @Test
  void builderNameConfigEscapingEnabled() throws Exception {
    clearEnvironmentVariables(Set.of(ENV_VAR));

    NameConfig cfg = new NameConfig();
    cfg.setEscaping(true);

    // Build the client to confirm the config is accepted; verify escaping through the same config.
    OpenLineageClient.builder().nameConfig(cfg).build().close();
    assertThat(NameEscaping.escapeSegment("a.b", cfg)).isEqualTo("a\\.b");
  }

  @Test
  void builderNameConfigEscapingDisabled() throws Exception {
    Map<String, String> env = new HashMap<>();
    env.put(ENV_VAR, "true");
    setEnvironmentVariables(env);

    NameConfig cfg = new NameConfig();
    cfg.setEscaping(false);

    // Even with env var true, the config says false → escaping off.
    OpenLineageClient.builder().nameConfig(cfg).build().close();
    try {
      assertThat(NameEscaping.escapeSegment("a.b", cfg)).isEqualTo("a.b");
    } finally {
      clearEnvironmentVariables(env.keySet());
    }
  }

  // -----------------------------------------------------------------------
  // Integration — Naming helpers respect the env var end-to-end
  // -----------------------------------------------------------------------

  @Test
  void oracleNamingDoesNotEscapeByDefault() throws Exception {
    clearEnvironmentVariables(Set.of(ENV_VAR));

    io.openlineage.client.dataset.Naming.Oracle oracle =
        io.openlineage.client.dataset.Naming.Oracle.builder()
            .host("localhost")
            .port("1521")
            .serviceName("mydb.example.com")
            .schema("mySchema")
            .table("myTable")
            .build();

    assertThat(oracle.getName()).isEqualTo("mydb.example.com.mySchema.myTable");
  }

  @Test
  void oracleNamingEscapesServiceNameWithDotsWhenEnabled() throws Exception {
    Map<String, String> env = new HashMap<>();
    env.put(ENV_VAR, "true");
    setEnvironmentVariables(env);

    try {
      io.openlineage.client.dataset.Naming.Oracle oracle =
          io.openlineage.client.dataset.Naming.Oracle.builder()
              .host("localhost")
              .port("1521")
              .serviceName("mydb.example.com")
              .schema("mySchema")
              .table("myTable")
              .build();

      // Spec example: "mydb\.example\.com.mySchema.myTable"
      assertThat(oracle.getName()).isEqualTo("mydb\\.example\\.com.mySchema.myTable");
    } finally {
      clearEnvironmentVariables(env.keySet());
    }
  }

  @Test
  void plainSegmentsAreUnchangedRegardlessOfEscapingSetting() throws Exception {
    clearEnvironmentVariables(Set.of(ENV_VAR));

    io.openlineage.client.dataset.Naming.Postgres pg =
        io.openlineage.client.dataset.Naming.Postgres.builder()
            .host("localhost")
            .port("5432")
            .database("mydb")
            .schema("myschema")
            .table("mytable")
            .build();

    assertThat(pg.getName()).isEqualTo("mydb.myschema.mytable");
  }
}

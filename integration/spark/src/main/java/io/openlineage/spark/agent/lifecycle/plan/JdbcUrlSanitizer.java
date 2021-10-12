package io.openlineage.spark.agent.lifecycle.plan;

public interface JdbcUrlSanitizer {
  String sanitize(String jdbcUrl);
}

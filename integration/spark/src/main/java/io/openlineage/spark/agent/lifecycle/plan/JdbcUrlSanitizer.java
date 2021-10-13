package io.openlineage.spark.agent.lifecycle.plan;

/**
 * JdbcUrl can contain username and password This interface clean-up credentials from jdbcUrl and
 * strip the jdbc prefix from the url
 */
public interface JdbcUrlSanitizer {

  String sanitize(String jdbcUrl);
}

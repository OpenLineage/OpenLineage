/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.job;

import lombok.Builder;
import lombok.NonNull;

/**
 * Utility class for constructing job names according to the OpenLineage job naming conventions.
 *
 * <p>Supported job types include:
 *
 * <ul>
 *   <li>Spark: {@code {appName}.{command}.{table}}
 *   <li>SQL: {@code {schema}.{table}}
 * </ul>
 */
public class Naming {

  private Naming() {
    // Utility class, do not instantiate
  }

  /** Interface representing a job name that can be resolved to a string. */
  public interface JobName {
    /**
     * Returns the formatted job name.
     *
     * @return a string representing the job name.
     */
    String getName();
  }

  /** Represents a Spark job name using the format: {@code {appName}.{command}.{table}}. */
  @Builder
  public static class Spark implements JobName {
    private final String appName;
    private final String command;
    private final String table;

    /**
     * Constructs a new {@link Spark} job name.
     *
     * @param appName the Spark application name; must be non-null and non-empty
     * @param command the command or function being run; must be non-null and non-empty
     * @param table the target table; must be non-null and non-empty
     * @throws IllegalArgumentException if any argument is empty
     * @throws NullPointerException if any argument is null
     */
    public Spark(@NonNull String appName, @NonNull String command, @NonNull String table) {
      if (appName.isEmpty() || command.isEmpty() || table.isEmpty()) {
        throw new IllegalArgumentException("appName, command, and table must be non-empty");
      }
      this.appName = appName;
      this.command = command;
      this.table = table;
    }

    /**
     * {@inheritDoc}
     *
     * @return the job name in the format: {@code {appName}.{command}.{table}}
     */
    @Override
    public String getName() {
      return String.format("%s.%s.%s", appName, command, table);
    }
  }

  /** Represents an SQL job name using the format: {@code {schema}.{table}}. */
  @Builder
  public static class SQL implements JobName {
    private final String schema;
    private final String table;

    /**
     * Constructs a new {@link SQL} job name.
     *
     * @param schema the schema name; must be non-null and non-empty
     * @param table the table name; must be non-null and non-empty
     * @throws IllegalArgumentException if any argument is empty
     * @throws NullPointerException if any argument is null
     */
    public SQL(@NonNull String schema, @NonNull String table) {
      if (schema.isEmpty() || table.isEmpty()) {
        throw new IllegalArgumentException("schema and table must be non-empty");
      }
      this.schema = schema;
      this.table = table;
    }

    /**
     * {@inheritDoc}
     *
     * @return the job name in the format: {@code {schema}.{table}}
     */
    @Override
    public String getName() {
      return String.format("%s.%s", schema, table);
    }
  }
}

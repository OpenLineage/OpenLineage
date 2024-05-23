/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Optional;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * By design, job name contains an appended dataset name from the output datasets. However, in many
 * cases outputs are not present for START events while the job name has to be assigned for START.
 * This method allows providing job name suffix by dataset builders. This can be table name in most
 * cases which can be exposed at START even if output dataset will be produced for COMPLETE
 *
 * @return
 */
public interface JobNameSuffixProvider<P extends LogicalPlan> {

  String SUFFIX_DELIMITER = "_";

  Optional<String> jobNameSuffix(OpenLineageContext context);

  default Optional<String> jobNameSuffix(P plan) {
    return Optional.empty();
  }

  default String tableIdentifierToSuffix(TableIdentifier identifier) {
    if (identifier.database().isDefined()) {
      return ScalaConversionUtils.asJavaOptional(identifier.database())
          .map(db -> db + "_" + identifier.identifier())
          .get();
    } else {
      return identifier.identifier();
    }
  }

  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  default String trimPath(String path) {
    if (path.lastIndexOf("/") > 0) {
      // is path
      String[] parts = path.split("/");
      if (parts.length >= 2) {
        // concat two last elements of the path
        return parts[parts.length - 2] + SUFFIX_DELIMITER + parts[parts.length - 1];
      } else {
        // get last path element
        return parts[parts.length - 1];
      }
    } else {
      // is something else
      return path;
    }
  }
}

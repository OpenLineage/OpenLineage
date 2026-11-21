/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import javax.annotation.Nullable;
import lombok.NonNull;

/**
 * Resolves the message / partition key of a run event from the {@link OpenLineage.ParentJobFacet}
 * of its job. It is used by transports as a fallback when the {@link OpenLineage.ParentRunFacet} is
 * not present, i.e. when the specific parent run is not known.
 */
public final class ParentJobKey {

  private ParentJobKey() {}

  /**
   * Returns {@code "run:<namespace>/<name>"} of the root job, or of the parent job when there is no
   * root, taken from {@code job.facets.parent}. Returns {@code null} when the facet is not present.
   */
  @Nullable
  public static String resolve(@NonNull OpenLineage.Job job) {
    final OpenLineage.JobFacets jobFacets = job.getFacets();
    if (jobFacets == null) {
      return null;
    }

    final OpenLineage.ParentJobFacet parentFacet = jobFacets.getParent();
    if (parentFacet == null) {
      return null;
    }

    final OpenLineage.ParentJobFacetRoot root = parentFacet.getRoot();
    if (root != null && root.getJob() != null) {
      return key(root.getJob());
    }

    final OpenLineage.ParentJobFacetJob parentJob = parentFacet.getJob();
    return parentJob == null ? null : key(parentJob);
  }

  private static String key(OpenLineage.ParentJobFacetJob job) {
    return "run:" + job.getNamespace() + "/" + job.getName();
  }
}

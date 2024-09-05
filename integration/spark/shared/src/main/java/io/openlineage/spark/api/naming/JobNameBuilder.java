/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api.naming;

import static io.openlineage.spark.agent.util.DatabricksUtils.prettifyDatabricksJobName;

import io.openlineage.spark.agent.util.DatabricksUtils;
import io.openlineage.spark.api.JobNameSuffixProvider;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.SparkOpenLineageConfig.JobNameConfig;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.WholeStageCodegenExec;

@Slf4j
public class JobNameBuilder {
  private static final String JOB_NAME_PARTS_SEPARATOR = ".";
  private static final String INNER_SEPARATOR = "_";
  private static final ApplicationJobNameResolver applicationJobNameResolver =
      new ApplicationJobNameResolver(ApplicationJobNameResolver.buildProvidersList());

  public static String build(OpenLineageContext context) {
    if (context.getJobName() != null) {
      return context.getJobName();
    }

    Optional<SparkConf> sparkConf = context.getSparkContext().map(SparkContext::getConf);
    StringBuilder jobNameBuilder =
        new StringBuilder(applicationJobNameResolver.getJobName(context));

    sparkNodeName(context)
        .ifPresent(
            nodeName ->
                jobNameBuilder
                    .append(JOB_NAME_PARTS_SEPARATOR)
                    .append(replaceDots(context, NameNormalizer.normalize(nodeName))));

    String jobName;
    if (context.getOpenLineageConfig().getJobName() != null
        && !context.getOpenLineageConfig().getJobName().getAppendDatasetName()) {
      // no need to append output dataset name
      jobName = jobNameBuilder.toString();
    } else {
      // append output dataset as job suffix
      jobNameBuilder.append(
          getJobSuffix(context)
              .map(
                  suffix ->
                      JOB_NAME_PARTS_SEPARATOR
                          + suffix.replace(JOB_NAME_PARTS_SEPARATOR, INNER_SEPARATOR))
              .orElse(""));

      jobName = jobNameBuilder.toString();
      if (sparkConf.isPresent() && DatabricksUtils.isRunOnDatabricksPlatform(sparkConf.get())) {
        jobName = prettifyDatabricksJobName(sparkConf.get(), jobName);
      }
    }

    context.setJobName(jobName);
    return jobName;
  }

  public static String build(OpenLineageContext context, String rddSuffix) {
    return applicationJobNameResolver.getJobName(context)
        + JOB_NAME_PARTS_SEPARATOR
        + NameNormalizer.normalize(rddSuffix);
  }

  private static String replaceDots(OpenLineageContext context, String jobName) {
    return Optional.of(context.getOpenLineageConfig())
        .map(SparkOpenLineageConfig::getJobName)
        .map(JobNameConfig::getReplaceDotWithUnderscore)
        .filter(Boolean::booleanValue)
        .map(b -> jobName.replace(".", "_"))
        .orElse(jobName);
  }

  private static Optional<String> getJobSuffix(OpenLineageContext context) {
    // load suffix providers from dataset builders
    List<JobNameSuffixProvider> suffixProviderList =
        context.getOutputDatasetBuilders().stream()
            .filter(b -> b instanceof JobNameSuffixProvider)
            .map(b -> (JobNameSuffixProvider) b)
            .collect(Collectors.toList());

    // load suffix providers from query plan visitors
    suffixProviderList.addAll(
        context.getOutputDatasetQueryPlanVisitors().stream()
            .filter(b -> b instanceof JobNameSuffixProvider)
            .map(b -> (JobNameSuffixProvider) b)
            .collect(Collectors.toList()));

    return suffixProviderList.stream()
        .map(p -> p.jobNameSuffix(context))
        .filter(Optional::isPresent)
        .map(s -> (String) s.get())
        .findFirst();
  }

  private static Optional<String> sparkNodeName(OpenLineageContext context) {
    if (context.getQueryExecution() == null
        || !context.getQueryExecution().isPresent()
        || context.getQueryExecution().get().executedPlan() == null) {
      return Optional.empty();
    }

    SparkPlan node = context.getQueryExecution().get().executedPlan();
    // Unwrap SparkPlan from WholeStageCodegen, as that's not a descriptive or helpful job name
    if (node instanceof WholeStageCodegenExec) {
      node = ((WholeStageCodegenExec) node).child();
    }

    return Optional.ofNullable(node).map(SparkPlan::nodeName).map(NameNormalizer::normalize);
  }
}

/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets;

import static io.openlineage.spark.agent.util.ScalaConversionUtils.asJavaOptional;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/** Captures information related to the Apache Spark application. */
@Getter
public class SparkApplicationDetailsFacet extends OpenLineage.DefaultRunFacet {
  @JsonProperty("master")
  @NonNull
  private String master;

  @JsonProperty("appName")
  @NonNull
  private String appName;

  @JsonProperty("applicationId")
  @NonNull
  private String applicationId;

  @JsonProperty("deployMode")
  @NonNull
  private String deployMode;

  @JsonProperty("driverHost")
  private String driverHost;

  @JsonProperty("userName")
  @NonNull
  private String userName;

  @JsonProperty("uiWebUrl")
  private String uiWebUrl;

  @JsonProperty("proxyUrl")
  private String proxyUrl;

  @JsonProperty("historyUrl")
  private String historyUrl;

  public SparkApplicationDetailsFacet(SparkContext sparkContext) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
    this.master = sparkContext.master();
    this.appName = sparkContext.appName();
    this.applicationId = sparkContext.applicationId();
    this.userName = sparkContext.sparkUser();
    // don't use spark.driver.appUIAddress because it is filled up only by Yarn
    this.uiWebUrl = asJavaOptional(sparkContext.uiWebUrl()).orElse(null);

    SparkConf conf = sparkContext.getConf();
    this.deployMode = conf.get("spark.submit.deployMode", "client");
    this.driverHost = conf.get("spark.driver.host", null);

    setProxyUrl(conf);
    setHistoryUrl(sparkContext);
  }

  private void setProxyUrl(SparkConf conf) {
    Optional<String> yarnProxyUrl =
        asJavaOptional(
            conf.getOption(
                "spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES"));

    if (yarnProxyUrl.isPresent()) {
      proxyUrl = yarnProxyUrl.get();
      return;
    }

    Boolean reverseProxyEnabled = conf.getBoolean("spark.ui.reverseProxy", false);
    if (!reverseProxyEnabled) {
      return;
    }

    String encodedId;
    try {
      encodedId = URLEncoder.encode(applicationId, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      return;
    }

    asJavaOptional(conf.getOption("spark.ui.reverseProxyUrl"))
        .ifPresent(
            reverseProxyUrl ->
                proxyUrl = StringUtils.stripEnd(reverseProxyUrl, "/") + "/proxy/" + encodedId);
  }

  private void setHistoryUrl(SparkContext sparkContext) {
    String encodedId;
    try {
      encodedId = URLEncoder.encode(applicationId, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      return;
    }

    SparkConf sparkConf = sparkContext.getConf();
    Configuration hadoopConf = sparkContext.hadoopConfiguration();
    asJavaOptional(sparkConf.getOption("spark.yarn.historyServer.address"))
        .ifPresent(
            historyServer -> {
              if (!historyServer.startsWith("http")) {
                String scheme =
                    StringUtils.lowerCase(
                        hadoopConf.get("yarn.http.policy", "HTTP_ONLY").replace("_ONLY", ""));
                historyServer = scheme + "://" + historyServer;
              }
              historyUrl = StringUtils.stripEnd(historyServer, "/") + "/history/" + encodedId;
            });
  }
}

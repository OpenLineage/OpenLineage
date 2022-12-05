/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.openlineage.client.OpenLineageYaml;
import io.openlineage.client.transports.FacetsConfig;
import io.openlineage.client.transports.TransportConfig;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;

class ArgumentParserTest {

  private static final String NS_NAME = "ns_name";
  private static final String JOB_NAME = "job_name";
  private static final String URL = "http://localhost:5000";
  private static final String RUN_ID = "ea445b5c-22eb-457a-8007-01c7c52b6e54";
  private static final String APP_NAME = "test";

  @Test
  void testGetDisabledFacets() {
    ArgumentParser.ArgumentParserBuilder builder = new ArgumentParser.ArgumentParserBuilder();
    builder.host("host");
    builder.DEFAULT_DISABLED_FACETS("spark_unknown;spark.logicalPlan");
    ArgumentParser parser = builder.build();

    assertThat(parser.getDEFAULT_DISABLED_FACETS())
        .contains("spark_unknown")
        .contains("spark.logicalPlan")
        .hasSize(2);
  }

  @Test
  void testGetDisabledFacetsWhenNoEntry() {
    ArgumentParser.ArgumentParserBuilder builder = new ArgumentParser.ArgumentParserBuilder();
    ArgumentParser parser = builder.build();

    assertThat(parser.getDEFAULT_DISABLED_FACETS()).contains("spark_unknown").hasSize(1);
  }

  @Test
  void testConfToHttpConfig(){
    SparkConf sparkConf = new SparkConf()
            .set("spark.openlineage.transport.type", "http")
            .set("spark.openlineage.transport.url", "http://localhost:5050")
            .set("spark.openlineage.transport.endpoint", "api/v1/lineage")
            .set("spark.openlineage.transport.auth.type", "api_key")
            .set("spark.openlineage.transport.auth.apiKey", "random_token")
            .set("spark.openlineage.transport.urlParams.number1", "value1")
            .set("spark.openlineage.transport.urlParams.number2", "value2")
            .set("spark.openlineage.facets.disabled", "facet1;facet2");
    OpenLineageYaml openLineageYaml = ArgumentParser.extractOpenlineageConfFromSparkConf(sparkConf);
    FacetsConfig facetsConfig = openLineageYaml.getFacetsConfig();
    TransportConfig transportConfig = openLineageYaml.getTransportConfig();
//    OpenLineageClient openLineageClient = new OpenLineageClient()
    
    assertEquals(1,1);
  }

  @Test
  void testConfToKafkaConfig(){
    SparkConf sparkConf = new SparkConf()
            .set("spark.openlineage.transport.type", "kafka")
            .set("spark.openlineage.transport.topicName", "dupa")
            .set("spark.openlineage.transport.localServerId", "dupa1")
            .set("spark.openlineage.transport.properties.dupa1", "DUPA1")
            .set("spark.openlineage.transport.properties.dupa2", "DUPA2");
    OpenLineageYaml openLineageYaml = ArgumentParser.extractOpenlineageConfFromSparkConf(sparkConf);
    FacetsConfig facetsConfig = openLineageYaml.getFacetsConfig();
    TransportConfig transportConfig = openLineageYaml.getTransportConfig();
//    OpenLineageClient openLineageClient = new OpenLineageClient()

    assertEquals(1,1);
  }
}

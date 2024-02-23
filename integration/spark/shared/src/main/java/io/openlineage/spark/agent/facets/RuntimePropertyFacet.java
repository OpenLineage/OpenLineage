/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/


package io.openlineage.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Arrays;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RuntimePropertyFacet extends OpenLineage.DefaultRunFacet {
    @JsonProperty("runtime_properties")
    private Map<String, Object> runtimeProperties;
    private static final String ALLOWED_KEY = "spark.openlineage.capturedRuntimeProperties";

    public Map<String, Object> getRuntimeProperties() {
        return runtimeProperties;
    }

    public RuntimePropertyFacet() {
        super(Versions.OPEN_LINEAGE_PRODUCER_URI);
        SparkSession session = SparkSession.active();
        runtimeProperties = new HashMap<>();
        try {
            Arrays.asList(session.conf().get(ALLOWED_KEY).split(",")).forEach(item -> trySetRuntimeProperty(session, item));
        } catch (NoSuchElementException e) {
            log.info("spark.openlineage.capturedRuntimeProperties is not set in RuntimeConfig");
        }

    }

    private void trySetRuntimeProperty(SparkSession session, String key) {
        try {
            runtimeProperties.putIfAbsent(key, session.conf().get(key));
        } catch (NoSuchElementException e) {
            log.info("A key in capturedRuntimeProperties not exists in Runtime Config", key);
        }
    }
}

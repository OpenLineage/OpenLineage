/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.unshaded.spark.extension.v1;

public interface OpenLineageExtensionProvider {

    String shadedPackage();

    default String getVisitorClassName(){
        return shadedPackage() + ".spark.extension.v1.lifecycle.plan.SparkOpenLineageExtensionVisitor";
    }
}

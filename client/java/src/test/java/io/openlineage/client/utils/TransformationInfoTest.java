/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import io.openlineage.client.OpenLineage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.*;
import static io.openlineage.client.utils.TransformationInfo.Types.DIRECT;
import static io.openlineage.client.utils.TransformationInfo.Types.INDIRECT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TransformationInfoTest {
    @Test
    void createsIdentity() {
        TransformationInfo transformation = TransformationInfo.identity();
        assertEquals(new TransformationInfo(
                    DIRECT,
                    IDENTITY,
                    "",
                    false),
                transformation);
    }

    @Test
    void createsNonMaskingTransformationByDefault() {
        TransformationInfo transformation = TransformationInfo.transformation();
        assertEquals(new TransformationInfo(
                        DIRECT,
                        TRANSFORMATION,
                        "",
                        false),
                transformation);
    }

    @ParameterizedTest
    @CsvSource({ "true", "false" })
    void createsTransformation(boolean isMasking) {
        TransformationInfo transformation = TransformationInfo.transformation(isMasking);
        assertEquals(new TransformationInfo(
                        DIRECT,
                        TRANSFORMATION,
                        "",
                        isMasking),
                transformation);
    }

    @Test
    void createsNonMaskingAggregationByDefault() {
        TransformationInfo transformation = TransformationInfo.aggregation();
        assertEquals(new TransformationInfo(
                        DIRECT,
                        AGGREGATION,
                        "",
                        false),
                transformation);
    }

    @ParameterizedTest
    @CsvSource({ "true", "false" })
    void createsAggregation(boolean isMasking) {
        TransformationInfo transformation = TransformationInfo.aggregation(isMasking);
        assertEquals(new TransformationInfo(
                        DIRECT,
                        AGGREGATION,
                        "",
                        isMasking),
                transformation);
    }

    @ParameterizedTest
    @EnumSource(TransformationInfo.Subtypes.class)
    void createsNonMaskingIndirectTransformationByDefault(TransformationInfo.Subtypes subtype) {
        TransformationInfo transformation = TransformationInfo.indirect(subtype);
        assertEquals(new TransformationInfo(
                        INDIRECT,
                        subtype,
                        "",
                        false),
                transformation);
    }

    @ParameterizedTest
    @EnumSource(TransformationInfo.Subtypes.class)
    void createsMaskingIndirectTransformation(TransformationInfo.Subtypes subtype) {
        TransformationInfo transformation = TransformationInfo.indirect(subtype, true);
        assertEquals(new TransformationInfo(
                        INDIRECT,
                        subtype,
                        "",
                        true),
                transformation);
    }

    @ParameterizedTest
    @EnumSource(TransformationInfo.Subtypes.class)
    void createsNonMaskingIndirectTransformation(TransformationInfo.Subtypes subtype) {
        TransformationInfo transformation = TransformationInfo.indirect(subtype, false);
        assertEquals(new TransformationInfo(
                        INDIRECT,
                        subtype,
                        "",
                        false),
                transformation);
    }

    @Test
    void canBeRepresentedAsInputFieldTransformations() {
        OpenLineage.InputFieldTransformations transformations = TransformationInfo.identity().toInputFieldsTransformations();
        
        assertEquals("DIRECT", transformations.getType());
        assertEquals("IDENTITY", transformations.getSubtype());
        assertEquals("", transformations.getDescription());
        assertEquals(false, transformations.getMasking());
    }

    @ParameterizedTest
    @MethodSource("mergeScenarios")
    void canMerge(TransformationInfo current, TransformationInfo another, TransformationInfo expectedOrNull) {
        if (expectedOrNull == null) {
            assertNull(current.merge(another));
        } else {
            TransformationInfo result = current.merge(another);
            assertEquals(expectedOrNull.getType(), result.getType());
            assertEquals(expectedOrNull.getSubType(), result.getSubType());
            assertEquals(expectedOrNull.getDescription(), result.getDescription());
            assertEquals(expectedOrNull.getMasking(), result.getMasking());
        }
    }

    private static Stream<Object[]> mergeScenarios() {
        return Stream.of(
                // 1. current is INDIRECT → keep all from current
                new Object[]{
                        new TransformationInfo(INDIRECT, IDENTITY, "desc", false),
                        new TransformationInfo(DIRECT, TRANSFORMATION, "new desc", false),
                        new TransformationInfo(INDIRECT, IDENTITY, "desc", false)
                },

                // 2. same as 1., but masking differs → keep all from current, result has masking true
                new Object[]{
                        new TransformationInfo(INDIRECT, IDENTITY, "desc", false),
                        new TransformationInfo(DIRECT, TRANSFORMATION, "new desc", true),
                        new TransformationInfo(INDIRECT, IDENTITY, "desc", true)
                },

                // 3. another == null → null
                new Object[]{
                        new TransformationInfo(DIRECT, IDENTITY, "desc", false),
                        null,
                        null
                },

                // 4. another is INDIRECT → keep all from another
                new Object[]{
                        new TransformationInfo(DIRECT, IDENTITY, "desc", false),
                        new TransformationInfo(INDIRECT, TRANSFORMATION, "new desc", false),
                        new TransformationInfo(INDIRECT, TRANSFORMATION, "new desc", false)
                },

                // 5. same as 4., but masking differs → keep all from another, result has masking true
                new Object[]{
                        new TransformationInfo(DIRECT, IDENTITY, "desc", true),
                        new TransformationInfo(INDIRECT, TRANSFORMATION, "new desc", false),
                        new TransformationInfo(INDIRECT, TRANSFORMATION, "new desc", true)
                },

                // 6. neither INDIRECT → pick lower ordinal
                new Object[]{
                        new TransformationInfo(DIRECT, IDENTITY, "desc", false),
                        new TransformationInfo(DIRECT, AGGREGATION, "new desc", false),
                        new TransformationInfo(DIRECT, AGGREGATION, "new desc", false)
                },

                // 7. same as 6., but masking differs → pick lower ordinal, result has masking true
                new Object[]{
                        new TransformationInfo(DIRECT, IDENTITY, "desc", true),
                        new TransformationInfo(DIRECT, AGGREGATION, "new desc", false),
                        new TransformationInfo(DIRECT, AGGREGATION, "new desc", true)
                },

                // 8. same ordinal → keep current
                new Object[]{
                        new TransformationInfo(DIRECT, IDENTITY, "desc", false),
                        new TransformationInfo(DIRECT, IDENTITY, "new desc", false),
                        new TransformationInfo(DIRECT, IDENTITY, "desc", false)
                },

                // 9. same as 8., but masking differs → keep current, result has masking true
                new Object[]{
                        new TransformationInfo(DIRECT, IDENTITY, "desc", false),
                        new TransformationInfo(DIRECT, IDENTITY, "new desc", true),
                        new TransformationInfo(DIRECT, IDENTITY, "desc", true)
                },

                // 10. masking same (true) → no changes to masking
                new Object[]{
                        new TransformationInfo(DIRECT, IDENTITY, "desc", true),
                        new TransformationInfo(DIRECT, AGGREGATION, "new desc", true),
                        new TransformationInfo(DIRECT, AGGREGATION, "new desc", true)
                }
        );
    }
}
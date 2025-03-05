/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet.schema;

import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public interface TypeInformationSchemaBuilder {

  boolean isDefinedAt(TypeInformation typeInformation);

  List<SchemaDatasetFacetFields> buildSchemaFields(TypeInformation typeInformation);
}

/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.lang.reflect.Method;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.Seq$;
import scala.collection.immutable.HashMap;

public class CatalogTableTestUtils {

  @SneakyThrows
  public static CatalogTable getCatalogTable(TableIdentifier tableIdentifier) {
    Method applyMethod =
        Arrays.stream(CatalogTable.class.getDeclaredMethods())
            .filter(m -> "apply".equals(m.getName()))
            .findFirst()
            .get();
    List<Object> params = new ArrayList<>();
    params.add(tableIdentifier);
    params.add(CatalogTableType.MANAGED());
    params.add(getCatalogStorageFormat());
    params.add(
        new StructType(
            new StructField[] {
              new StructField("name", StringType$.MODULE$, false, Metadata.empty())
            }));
    params.add(Option.empty());
    params.add(ScalaConversionUtils.fromList(Collections.singletonList("name")));
    params.add(Option.empty());
    params.add("");
    params.add(Instant.now().getEpochSecond());
    params.add(Instant.now().getEpochSecond());
    params.add("v1");
    params.add(new HashMap<>());
    params.add(Option.empty());
    params.add(Option.empty());
    params.add(Option.empty());
    if (Option.class.equals(applyMethod.getParameterTypes()[params.size()])) {
      params.add(Option.empty());
    }
    params.add(Seq$.MODULE$.<String>empty());
    params.add(false);
    params.add(false);
    params.add(new HashMap<>());
    while (applyMethod.getParameterCount() > params.size()) {
      params.add(Option.empty());
    }
    return (CatalogTable) applyMethod.invoke(null, params.toArray());
  }

  private static CatalogStorageFormat getCatalogStorageFormat() throws Exception {
    Method applyMethod =
        Arrays.stream(CatalogStorageFormat.class.getDeclaredMethods())
            .filter(m -> "apply".equals(m.getName()))
            .filter(m -> CatalogStorageFormat.class.equals(m.getReturnType()))
            .findFirst()
            .get();
    List<Object> params =
        new ArrayList<>(
            Arrays.asList(
                Option.apply(new URI("/some/location")),
                Option.empty(),
                Option.empty(),
                Option.empty(),
                false,
                new HashMap<>()));
    if (applyMethod.getParameterCount() > params.size()) {
      params.add(Option.empty());
    }
    return (CatalogStorageFormat) applyMethod.invoke(null, params.toArray());
  }
}

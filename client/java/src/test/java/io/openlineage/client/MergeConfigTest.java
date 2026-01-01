/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import static io.openlineage.client.MergeConfigTest.MergeConfigClass.PRIMITIVE_CONST;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MergeConfigTest {

  MergeConfigClass o1;
  MergeConfigClass o2;

  @BeforeEach
  void before() {
    o1 = new MergeConfigClass();
    o2 = new MergeConfigClass();
  }

  @Test
  void testNullMap() {
    o2.map = Collections.singletonMap("k", "v");

    MergeConfigClass o = o1.mergeWith(o2);
    assertThat(o.map).hasSize(1).containsEntry("k", "v");

    o = o2.mergeWith(o1);
    assertThat(o.map).hasSize(1).containsEntry("k", "v");
  }

  @Test
  void testMapIsCombined() {
    o1.map = Collections.singletonMap("k1", "v1");
    o2.map = Collections.singletonMap("k2", "v2");

    MergeConfigClass o = o1.mergeWith(o2);
    assertThat(o.map).hasSize(2).containsEntry("k1", "v1").containsEntry("k2", "v2");
  }

  @Test
  void testObjectIsOverwritten() {
    Object someObject = new Object();
    o2.object = someObject;

    MergeConfigClass o = o1.mergeWith(o2);
    assertThat(o.object).isEqualTo(someObject);

    o = o2.mergeWith(o1);
    assertThat(o.object).isEqualTo(someObject);
  }

  @Test
  void testPrimitiveIsOverwritten() {
    o1.primitive = 30;
    o2.primitive = 40;

    MergeConfigClass o = o1.mergeWith(o2);
    assertThat(o.primitive).isEqualTo(40);
  }

  @Test
  void testPrimitiveIsNotOverwrittenWithDefault() {
    o1.primitive = 30;
    o2.primitive = PRIMITIVE_CONST;

    MergeConfigClass o = o1.mergeWith(o2);
    assertThat(o.primitive).isEqualTo(30);
  }

  @Test
  void testDeepMerge() {
    DeeperMergeConfigClass deep1 = new DeeperMergeConfigClass();
    DeeperMergeConfigClass deep2 = new DeeperMergeConfigClass();

    deep1.o = "a";
    deep2.o = "b";

    o1.deepConfig = deep1;
    o2.deepConfig = deep2;

    MergeConfigClass o = o1.mergeWith(o2);
    assertThat(((DeeperMergeConfigClass) o.deepConfig).o.toString()).isEqualTo("b");
  }

  @Test
  void testDeepWithNull() {
    DeeperMergeConfigClass deep1 = new DeeperMergeConfigClass();
    DeeperMergeConfigClass deep2 = new DeeperMergeConfigClass();

    deep1.o = "a";

    o1.deepConfig = deep1;
    o2.deepConfig = deep2;

    o1.mergeWith(o2);
    assertThat(((DeeperMergeConfigClass) o1.deepConfig).o.toString()).isEqualTo("a");
  }

  @Test
  void testDeepMergeWhenTypesDoNotMatch() {
    DeeperMergeConfigClass deep1 = new DeeperMergeConfigClass();
    OtherDeeperMergeConfigClass deep2 = new OtherDeeperMergeConfigClass();

    deep1.o = "a";
    deep2.o = "b";

    o1.deepConfig = deep1;
    o2.deepConfig = deep2;

    MergeConfigClass o = o1.mergeWith(o2);
    assertThat(((OtherDeeperMergeConfigClass) o.deepConfig).o).isEqualTo("b");
  }

  static class MergeConfigClass implements MergeConfig<MergeConfigClass> {
    static Integer PRIMITIVE_CONST = 10;
    Object object;
    Map map;
    Integer primitive;
    String string;
    Object deepConfig;

    @Override
    public MergeConfigClass mergeWithNonNull(MergeConfigClass mergeConfigClass) {
      MergeConfigClass o = new MergeConfigClass();

      o.object = mergePropertyWith(object, mergeConfigClass.object);
      o.map = mergePropertyWith(map, mergeConfigClass.map);
      o.primitive = mergeWithDefaultValue(primitive, mergeConfigClass.primitive, PRIMITIVE_CONST);
      o.string = mergePropertyWith(string, mergeConfigClass.string);
      o.deepConfig = mergePropertyWith(deepConfig, mergeConfigClass.deepConfig);

      return o;
    }
  }

  static class DeeperMergeConfigClass implements MergeConfig<DeeperMergeConfigClass> {
    Object o;

    @Override
    public DeeperMergeConfigClass mergeWithNonNull(DeeperMergeConfigClass deeperMergeConfigClass) {
      DeeperMergeConfigClass d = new DeeperMergeConfigClass();
      d.o = mergePropertyWith(o, deeperMergeConfigClass.o);
      return d;
    }
  }

  static class OtherDeeperMergeConfigClass implements MergeConfig<OtherDeeperMergeConfigClass> {

    Object o;

    @Override
    public OtherDeeperMergeConfigClass mergeWithNonNull(
        OtherDeeperMergeConfigClass otherDeeperMergeConfigClass) {
      OtherDeeperMergeConfigClass d = new OtherDeeperMergeConfigClass();
      d.o = mergePropertyWith(o, otherDeeperMergeConfigClass.o);
      return d;
    }
  }
}

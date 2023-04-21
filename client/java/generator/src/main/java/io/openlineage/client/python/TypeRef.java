package io.openlineage.client.python;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@Builder
@Getter
@RequiredArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class TypeRef {
  public static TypeRef INT = TypeRef.builder().name("int").isPrimitive(true).build();
  public static TypeRef STR = TypeRef.builder().name("str").isPrimitive(true).build();
  public static TypeRef FLOAT = TypeRef.builder().name("float").isPrimitive(true).build();
  public static TypeRef BOOL = TypeRef.builder().name("bool").isPrimitive(true).build();
  public static TypeRef DATETIME =
      TypeRef.builder()
          .name("datetime")
          .module("datetime")
          .isPrimitive(false)
          .isInternal(false)
          .build();

  @NonNull String name;
  String module;
  boolean sameFile;
  boolean isPrimitive;
  boolean isInternal;

  TypeRef arrayRef;
  TypeRef keyRef;
  TypeRef valueRef;
}

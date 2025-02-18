/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.run;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.openlineage.client.MergeConfig;
import io.openlineage.client.utils.TagField;
import io.openlineage.client.utils.TagsDeserializer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class RunConfig implements MergeConfig<RunConfig> {
  @JsonDeserialize(using = TagsDeserializer.class)
  private List<TagField> tags = Collections.emptyList();

  @Override
  public RunConfig mergeWithNonNull(RunConfig other) {
    Map<String, TagField> tagMap =
        tags.stream().collect(Collectors.toMap(TagField::getKey, x -> x));
    other.getTags().forEach(tag -> tagMap.put(tag.getKey(), tag));
    RunConfig runConfig = new RunConfig();
    runConfig.setTags(new ArrayList<>(tagMap.values()));
    return runConfig;
  }
}

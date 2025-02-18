/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.job;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.openlineage.client.MergeConfig;
import io.openlineage.client.utils.TagField;
import io.openlineage.client.utils.TagsDeserializer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

@Getter
@Setter
@ToString
public class JobConfig implements MergeConfig<JobConfig> {
  private JobOwnersConfig owners;
  private String namespace;
  private String name;

  @JsonDeserialize(using = TagsDeserializer.class)
  private List<TagField> tags = Collections.emptyList();

  @Override
  public JobConfig mergeWithNonNull(JobConfig other) {
    Map<String, TagField> tagMap = new HashMap<>();

    if (tags != null) {
      tags.forEach(tag -> tagMap.put(tag.getKey(), tag));
    }

    if (other.getTags() != null) {
      other.getTags().forEach(tag -> tagMap.put(tag.getKey(), tag));
    }

    JobConfig jobConfig = new JobConfig();

    JobOwnersConfig newOwners = new JobOwnersConfig();
    newOwners.getAdditionalProperties().putAll(owners.getAdditionalProperties());
    newOwners.getAdditionalProperties().putAll(other.owners.getAdditionalProperties());
    jobConfig.setOwners(newOwners);
    jobConfig.setTags(new ArrayList<>(tagMap.values()));

    jobConfig.name = StringUtils.isNotBlank(other.name) ? other.name : name;
    jobConfig.namespace = StringUtils.isNotBlank(other.namespace) ? other.namespace : namespace;
    return jobConfig;
  }

  @Getter
  @ToString
  public static class JobOwnersConfig {
    @JsonAnySetter @NonNull
    private final Map<String, String> additionalProperties = new HashMap<>();
  }
}

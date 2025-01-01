/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.cassandra;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

@Table(keyspace = "flink", name = "sink_event")
public class Event implements Serializable {
  private static final long serialVersionUID = 1L;

  @Column(name = "id")
  private UUID id;

  @Column(name = "content")
  private String content;

  @Column(name = "timestamp")
  private long timestamp;

  public Event() {}

  public Event(UUID id, String content, long timestamp) {
    this.id = id;
    this.content = content;
    this.timestamp = timestamp;
  }

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public String getContent() {
    return content;
  }

  public void setContent(String content) {
    this.content = content;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Event event = (Event) o;
    return timestamp == event.timestamp && id.equals(event.id) && content.equals(event.content);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, content, timestamp);
  }
}

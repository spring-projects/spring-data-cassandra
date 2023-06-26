package org.springframework.data.cassandra.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.mapping.Table;

@Table(value = "entity_with_keyspace", keyspace = "custom")
public record EntityWithKeyspace(
  @Id String id,
  String name,
  String type
){}
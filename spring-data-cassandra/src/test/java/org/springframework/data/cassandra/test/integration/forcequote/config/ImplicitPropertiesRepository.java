package org.springframework.data.cassandra.test.integration.forcequote.config;

import org.springframework.data.cassandra.repository.TypedIdCassandraRepository;

public interface ImplicitPropertiesRepository extends TypedIdCassandraRepository<ImplicitProperties, String> {}

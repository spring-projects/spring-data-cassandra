package org.springframework.data.cassandra.test.integration.forcequote.config;

import org.springframework.data.cassandra.repository.CassandraRepository;

public interface ImplicitPropertiesRepository extends CassandraRepository<ImplicitProperties, String> {
}

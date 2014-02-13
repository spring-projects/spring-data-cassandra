package org.springframework.data.cassandra.test.integration.minimal.config.entities;

import org.springframework.data.cassandra.repository.CassandraRepository;

public interface AbsMinRepository extends CassandraRepository<AbsMin, String> {
}

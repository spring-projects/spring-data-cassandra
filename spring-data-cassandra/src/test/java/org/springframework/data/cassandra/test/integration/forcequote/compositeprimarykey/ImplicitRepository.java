package org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey;

import org.springframework.data.cassandra.repository.CassandraRepository;

public interface ImplicitRepository extends CassandraRepository<Implicit, ImplicitKey> {
}

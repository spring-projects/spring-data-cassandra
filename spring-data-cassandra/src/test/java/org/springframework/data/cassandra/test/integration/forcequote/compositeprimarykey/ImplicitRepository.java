package org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey;

import org.springframework.data.cassandra.repository.TypedIdCassandraRepository;

public interface ImplicitRepository extends TypedIdCassandraRepository<Implicit, ImplicitKey> {}

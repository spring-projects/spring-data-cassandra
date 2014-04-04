package org.springframework.data.cassandra.test.integration.querymethods.intparam;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;

public interface ThingRepo extends CassandraRepository<Thing> {

	@Query("SELECT * from thing where number = ?0")
	Thing findThingByIntPrimitive(int number);

	@Query("SELECT * from thing where number = ?0")
	Thing findThingByIntReference(Integer number);
}

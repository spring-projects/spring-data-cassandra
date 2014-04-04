package org.springframework.data.cassandra.test.integration.querymethods.datekey;

import java.util.Date;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;

public interface ThingRepo extends CassandraRepository<Thing> {

	@Query("SELECT * from thing where date = ?0")
	Thing findThingByDate(Date date);
}

package org.springframework.data.cassandra.test.integration.querymethods.bigintparam;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;

import java.math.BigInteger;

public interface BigThingRepo extends CassandraRepository<BigThing> {

	@Query("SELECT * from bigthing where number = ?0")
	BigThing findThingByBigInteger(BigInteger number);

}

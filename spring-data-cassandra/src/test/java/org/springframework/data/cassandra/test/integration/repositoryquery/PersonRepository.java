package org.springframework.data.cassandra.test.integration.repositoryquery;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;

import com.datastax.driver.core.ResultSet;

public interface PersonRepository extends CassandraRepository<Person> {

	@Query("select * from person where lastname = '?0'")
	List<Person> findFolksWithLastnameAsList(String lastname);

	@Query("select * from person where lastname = '?0'")
	ResultSet findFolksWithLastnameAsResultSet(String last);

	@Query("select * from person where lastname = '?0'")
	Person[] findFolksWithLastnameAsArray(String lastname);

	@Query("select * from person where lastname = '?0' and firstname = '?1'")
	Person findSingle(String last, String first);

	@Query("select * from person where lastname = '?0'")
	List<Map<String, Object>> findFolksWithLastnameAsListOfMapOfStringToObject(String last);

	@Query("select nickname from person where lastname = '?0' and firstname = '?1'")
	String findSingleNickname(String last, String first);

	@Query("select birthdate from person where lastname = '?0' and firstname = '?1'")
	Date findSingleBirthdate(String last, String first);

	@Query("select cool from person where lastname = '?0' and firstname = '?1'")
	boolean findSingleCool(String last, String first);

	@Query("select numberofchildren from person where lastname = '?0' and firstname = '?1'")
	int findSingleNumberOfChildren(String last, String first);

	@Query("select uuid from person where lastname = '?0' and firstname = '?1'")
	UUID findSingleUuid(String last, String first);
}

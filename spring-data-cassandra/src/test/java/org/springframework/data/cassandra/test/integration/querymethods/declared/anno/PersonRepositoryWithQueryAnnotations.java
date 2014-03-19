package org.springframework.data.cassandra.test.integration.querymethods.declared.anno;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cassandra.test.integration.querymethods.declared.Person;
import org.springframework.data.cassandra.test.integration.querymethods.declared.base.PersonRepository;
import org.springframework.data.cassandra.test.integration.querymethods.declared.named.PersonRepositoryWithNamedQueries;

import com.datastax.driver.core.ResultSet;

/**
 * we extend {@link PersonRepositoryWithNamedQueries} here just to keep the test codebase in sync.
 */
public interface PersonRepositoryWithQueryAnnotations extends PersonRepository {

	@Override
	@Query("select * from person where lastname = '?0'")
	List<Person> findFolksWithLastnameAsList(String lastname);

	@Override
	@Query("select * from person where lastname = '?0'")
	ResultSet findFolksWithLastnameAsResultSet(String last);

	@Override
	@Query("select * from person where lastname = '?0'")
	Person[] findFolksWithLastnameAsArray(String lastname);

	@Override
	@Query("select * from person where lastname = '?0' and firstname = '?1'")
	Person findSingle(String last, String first);

	@Override
	@Query("select * from person where lastname = '?0'")
	List<Map<String, Object>> findFolksWithLastnameAsListOfMapOfStringToObject(String last);

	@Override
	@Query("select nickname from person where lastname = '?0' and firstname = '?1'")
	String findSingleNickname(String last, String first);

	@Override
	@Query("select birthdate from person where lastname = '?0' and firstname = '?1'")
	Date findSingleBirthdate(String last, String first);

	@Override
	@Query("select cool from person where lastname = '?0' and firstname = '?1'")
	boolean findSingleCool(String last, String first);

	@Override
	@Query("select numberofchildren from person where lastname = '?0' and firstname = '?1'")
	int findSingleNumberOfChildren(String last, String first);
}

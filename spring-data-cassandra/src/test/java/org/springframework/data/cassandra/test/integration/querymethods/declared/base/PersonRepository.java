package org.springframework.data.cassandra.test.integration.querymethods.declared.base;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.test.integration.querymethods.declared.Person;
import org.springframework.data.repository.NoRepositoryBean;

import com.datastax.driver.core.ResultSet;

@NoRepositoryBean
public interface PersonRepository extends CassandraRepository<Person> {

	List<Person> findFolksWithLastnameAsList(String lastname);

	ResultSet findFolksWithLastnameAsResultSet(String last);

	Person[] findFolksWithLastnameAsArray(String lastname);

	Person findSingle(String last, String first);

	List<Map<String, Object>> findFolksWithLastnameAsListOfMapOfStringToObject(String last);

	String findSingleNickname(String last, String first);

	Date findSingleBirthdate(String last, String first);

	boolean findSingleCool(String last, String first);

	int findSingleNumberOfChildren(String last, String first);
}

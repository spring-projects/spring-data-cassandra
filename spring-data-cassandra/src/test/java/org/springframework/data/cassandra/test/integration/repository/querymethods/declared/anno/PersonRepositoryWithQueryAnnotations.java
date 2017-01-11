/*
 * Copyright 2016-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.test.integration.repository.querymethods.declared.anno;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cassandra.test.integration.repository.querymethods.declared.Person;
import org.springframework.data.cassandra.test.integration.repository.querymethods.declared.base.PersonRepository;
import org.springframework.data.cassandra.test.integration.repository.querymethods.declared.named.PersonRepositoryWithNamedQueries;
import org.springframework.data.repository.query.Param;

import com.datastax.driver.core.ResultSet;

/**
 * we extend {@link PersonRepositoryWithNamedQueries} here just to keep the test codebase in sync.
 *
 * @author Matthew T. Adams
 */
public interface PersonRepositoryWithQueryAnnotations extends PersonRepository {

	@Override
	@Query("select * from person where lastname = ?0")
	List<Person> findFolksWithLastnameAsList(String lastname);

	@Override
	@Query("select * from person where lastname = ?0")
	ResultSet findFolksWithLastnameAsResultSet(String last);

	@Override
	@Query("select * from person where lastname = ?0")
	Person[] findFolksWithLastnameAsArray(String lastname);

	@Override
	@Query("select * from person where lastname = ?#{[0]} and firstname = ?1")
	Person findSingle(String last, String first);

	@Override
	@Query("select * from person where lastname = :last")
	List<Map<String, Object>> findFolksWithLastnameAsListOfMapOfStringToObject(@Param("last") String last);

	@Override
	@Query("select nickname from person where lastname = :#{#last} and firstname = ?1")
	String findSingleNickname(@Param("last") String last, @Param("first") String first);

	@Override
	@Query("select birthdate from person where lastname = ?0 and firstname = ?1")
	Date findSingleBirthdate(String last, String first);

	@Override
	@Query("select cool from person where lastname = ?0 and firstname = ?1")
	boolean findSingleCool(String last, String first);

	@Override
	@Query("select numberofchildren from person where lastname = ?0 and firstname = ?1")
	int findSingleNumberOfChildren(String last, String first);

	@Override
	@Query("select * from person where lastname = ?0 and firstname = ?1")
	Optional<Person> findOptionalWithLastnameAndFirstname(String last, String first);

	@Override
	@Query("select * from person")
	Stream<Person> findAllPeople();

}

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
package org.springframework.data.cassandra.test.integration.repository.querymethods.declared.base;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.test.integration.repository.querymethods.declared.Person;
import org.springframework.data.repository.NoRepositoryBean;

import com.datastax.driver.core.ResultSet;

/**
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
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

	Optional<Person> findOptionalWithLastnameAndFirstname(String last, String first);

	Stream<Person> findAllPeople();
}

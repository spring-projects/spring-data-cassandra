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
package org.springframework.data.cassandra.test.integration.repository.querymethods.derived;

import java.time.LocalDate;
import java.util.Collection;
import java.util.List;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cassandra.test.integration.repository.querymethods.declared.Address;
import org.springframework.data.cassandra.test.integration.repository.querymethods.declared.Person;
import org.springframework.data.domain.Sort;

/**
 * @author Mark Paluch
 */
interface PersonRepository extends CassandraRepository<Person> {

	List<Person> findByLastname(String lastname);

	List<Person> findByLastname(String lastname, Sort sort);

	List<Person> findByLastnameOrderByFirstnameAsc(String lastname);

	Person findByFirstnameAndLastname(String firstname, String lastname);

	Person findByMainAddress(Address address);

	@Query("select * from person where mainaddress = ?0")
	Person findByAddress(Address address);

	Person findByCreatedDate(LocalDate createdDate);

	Person findByNicknameStartsWith(String prefix);

	Person findByNicknameContains(String contains);

	Person findByNumberOfChildren(NumberOfChildren numberOfChildren);

	Collection<PersonProjection> findPersonProjectedBy();

	Collection<PersonDto> findPersonDtoBy();

	<T> T findDtoByNicknameStartsWith(String prefix, Class<T> projectionType);

	@Query("select * from person where firstname = ?0 and lastname = 'White'")
	List<Person> findByFirstname(String firstname);

	enum NumberOfChildren {
		ZERO, ONE, TWO,
	}

	interface PersonProjection {

		String getFirstname();

		String getLastname();
	}

	class PersonDto {

		public String firstname, lastname;

		public PersonDto(String firstname, String lastname) {
			this.firstname = firstname;
			this.lastname = lastname;
		}
	}
}

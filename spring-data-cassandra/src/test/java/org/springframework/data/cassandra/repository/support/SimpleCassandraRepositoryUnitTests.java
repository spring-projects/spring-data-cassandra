/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.cassandra.repository.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;

import lombok.Data;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.cql.CqlOperations;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.domain.Sort.Direction;

import com.datastax.driver.core.UserType;
import com.datastax.driver.core.querybuilder.Insert;

/**
 * Unit tests for {@link SimpleCassandraRepository}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class SimpleCassandraRepositoryUnitTests {

	CassandraMappingContext mappingContext = new CassandraMappingContext();
	MappingCassandraConverter converter = new MappingCassandraConverter(mappingContext);

	SimpleCassandraRepository<Object, ? extends Serializable> repository;

	@Mock CassandraOperations cassandraOperations;
	@Mock CqlOperations cqlOperations;
	@Mock UserTypeResolver userTypeResolver;
	@Mock UserType userType;

	@Captor ArgumentCaptor<Insert> insertCaptor;

	@Before
	public void before() {

		mappingContext.setUserTypeResolver(userTypeResolver);

		when(cassandraOperations.getConverter()).thenReturn(converter);
		when(cassandraOperations.getCqlOperations()).thenReturn(cqlOperations);
		when(userTypeResolver.resolveType(CqlIdentifier.of("address"))).thenReturn(userType);
	}

	@Test // DATACASS-428
	public void saveShouldInsertNewPrimaryKeyOnlyEntity() {

		CassandraPersistentEntity<?> entity = converter.getMappingContext().getRequiredPersistentEntity(SimplePerson.class);

		repository = new SimpleCassandraRepository<Object, String>(new MappingCassandraEntityInformation(entity, converter),
				cassandraOperations);

		SimplePerson person = new SimplePerson();

		repository.save(person);

		verify(cqlOperations).execute(insertCaptor.capture());
		assertThat(insertCaptor.getValue().toString()).isEqualTo("INSERT INTO simpleperson (id) VALUES (null);");
	}

	@Test // DATACASS-428
	public void saveShouldUpdateNewEntity() {

		CassandraPersistentEntity<?> entity = converter.getMappingContext().getRequiredPersistentEntity(Person.class);

		repository = new SimpleCassandraRepository<Object, String>(new MappingCassandraEntityInformation(entity, converter),
				cassandraOperations);

		Person person = new Person();

		repository.save(person);

		verify(cqlOperations).execute(any(Insert.class));
	}

	@Test // DATACASS-428
	public void saveShouldUpdateExistingEntity() {

		CassandraPersistentEntity<?> entity = converter.getMappingContext().getRequiredPersistentEntity(Person.class);

		repository = new SimpleCassandraRepository<Object, String>(new MappingCassandraEntityInformation(entity, converter),
				cassandraOperations);

		Person person = new Person();
		person.setFirstname("foo");
		person.setLastname("bar");

		repository.save(person);

		verify(cqlOperations).execute(insertCaptor.capture());
		assertThat(insertCaptor.getValue().toString())
				.contains("INSERT INTO person (lastname,firstname,alternativeaddresses,");
		assertThat(insertCaptor.getValue().toString()).contains("VALUES ('bar','foo',null");
	}

	@Test // DATACASS-428
	public void insertShouldInsertEntity() {

		CassandraPersistentEntity<?> entity = converter.getMappingContext().getRequiredPersistentEntity(Person.class);

		repository = new SimpleCassandraRepository<Object, String>(new MappingCassandraEntityInformation(entity, converter),
				cassandraOperations);

		Person person = new Person();

		repository.insert(person);

		verify(cassandraOperations).insert(person);
	}

	@Test // DATACASS-56
	public void shouldSelectWithPaging() {

		CassandraPageRequest pageRequest = CassandraPageRequest.first(10, Direction.ASC, "foo");

		repository = new SimpleCassandraRepository<Object, String>(
				new MappingCassandraEntityInformation(
						converter.getMappingContext().getRequiredPersistentEntity(SimplePerson.class), converter),
				cassandraOperations);

		repository.findAll(pageRequest);

		verify(cassandraOperations).slice(
				Query.empty().sort(pageRequest.getSort()).queryOptions(QueryOptions.builder().fetchSize(10).build()),
				SimplePerson.class);
	}

	@Data
	static class SimplePerson {

		@Id String id;
	}
}

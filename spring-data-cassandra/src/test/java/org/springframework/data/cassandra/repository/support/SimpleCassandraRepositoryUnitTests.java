/*
 * Copyright 2017-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.repository.support;

import static org.mockito.Mockito.*;

import lombok.Data;

import java.io.Serializable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.EntityWriteResult;
import org.springframework.data.cassandra.core.InsertOptions;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.CqlOperations;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.domain.Sort.Direction;

import com.datastax.oss.driver.api.core.type.UserDefinedType;

/**
 * Unit tests for {@link SimpleCassandraRepository}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
class SimpleCassandraRepositoryUnitTests {

	private CassandraMappingContext mappingContext = new CassandraMappingContext();

	private MappingCassandraConverter converter = new MappingCassandraConverter(mappingContext);

	private SimpleCassandraRepository<Object, ? extends Serializable> repository;

	@Mock CassandraOperations cassandraOperations;
	@Mock CqlOperations cqlOperations;
	@Mock UserDefinedType userType;
	@Mock UserTypeResolver userTypeResolver;
	@Mock EntityWriteResult writeResult;

	@BeforeEach
	void before() {
		mappingContext.setUserTypeResolver(userTypeResolver);
		when(cassandraOperations.getConverter()).thenReturn(converter);
	}

	@Test // DATACASS-428, DATACASS-560, DATACASS-573
	void saveShouldInsertNewPrimaryKeyOnlyEntity() {

		CassandraPersistentEntity<?> entity = converter.getMappingContext().getRequiredPersistentEntity(SimplePerson.class);

		repository = new SimpleCassandraRepository<Object, String>(new MappingCassandraEntityInformation(entity, converter),
				cassandraOperations);

		SimplePerson person = new SimplePerson();

		when(cassandraOperations.insert(eq(person), any())).thenReturn(writeResult);
		when(writeResult.getEntity()).thenReturn(person);

		repository.save(person);

		verify(cassandraOperations).insert(person, InsertOptions.builder().withInsertNulls().build());
	}

	@Test // DATACASS-576
	void shouldInsertNewVersionedEntity() {

		when(cassandraOperations.insert(any(), any(InsertOptions.class))).thenReturn(writeResult);

		CassandraPersistentEntity<?> entity = converter.getMappingContext()
				.getRequiredPersistentEntity(VersionedPerson.class);

		repository = new SimpleCassandraRepository<Object, String>(new MappingCassandraEntityInformation(entity, converter),
				cassandraOperations);

		VersionedPerson versionedPerson = new VersionedPerson();

		repository.save(versionedPerson);

		verify(cassandraOperations).insert(versionedPerson, InsertOptions.builder().withInsertNulls().build());
	}

	@Test // DATACASS-576
	void shouldUpdateExistingVersionedEntity() {

		CassandraPersistentEntity<?> entity = converter.getMappingContext()
				.getRequiredPersistentEntity(VersionedPerson.class);

		repository = new SimpleCassandraRepository<Object, String>(new MappingCassandraEntityInformation(entity, converter),
				cassandraOperations);

		VersionedPerson versionedPerson = new VersionedPerson();

		versionedPerson.setVersion(2);

		repository.save(versionedPerson);

		verify(cassandraOperations).update(versionedPerson);
	}

	@Test // DATACASS-428, DATACASS-560, DATACASS-573
	void saveShouldUpdateNewEntity() {

		CassandraPersistentEntity<?> entity = converter.getMappingContext().getRequiredPersistentEntity(Person.class);

		repository = new SimpleCassandraRepository<Object, String>(new MappingCassandraEntityInformation(entity, converter),
				cassandraOperations);

		Person person = new Person();

		when(cassandraOperations.insert(eq(person), any())).thenReturn(writeResult);
		when(writeResult.getEntity()).thenReturn(person);

		repository.save(person);

		verify(cassandraOperations).insert(person, InsertOptions.builder().withInsertNulls().build());
	}

	@Test // DATACASS-428, DATACASS-560, DATACASS-573
	void saveShouldUpdateExistingEntity() {

		CassandraPersistentEntity<?> entity = converter.getMappingContext().getRequiredPersistentEntity(Person.class);

		repository = new SimpleCassandraRepository<Object, String>(new MappingCassandraEntityInformation(entity, converter),
				cassandraOperations);

		Person person = new Person();

		person.setFirstname("foo");
		person.setLastname("bar");

		when(cassandraOperations.insert(eq(person), any())).thenReturn(writeResult);
		when(writeResult.getEntity()).thenReturn(person);

		repository.save(person);

		verify(cassandraOperations).insert(person, InsertOptions.builder().withInsertNulls().build());
	}

	@Test // DATACASS-428
	void insertShouldInsertEntity() {

		CassandraPersistentEntity<?> entity = converter.getMappingContext().getRequiredPersistentEntity(Person.class);

		repository = new SimpleCassandraRepository<Object, String>(new MappingCassandraEntityInformation(entity, converter),
				cassandraOperations);

		Person person = new Person();

		repository.insert(person);

		verify(cassandraOperations).insert(person);
	}

	@Test // DATACASS-56
	void shouldSelectWithPaging() {

		CassandraPageRequest pageRequest = CassandraPageRequest.first(10, Direction.ASC, "foo");

		repository = new SimpleCassandraRepository<Object, String>(
				new MappingCassandraEntityInformation(
						converter.getMappingContext().getRequiredPersistentEntity(SimplePerson.class), converter),
				cassandraOperations);

		repository.findAll(pageRequest);

		verify(cassandraOperations).slice(
				Query.empty().sort(pageRequest.getSort()).queryOptions(QueryOptions.builder().pageSize(10).build()),
				SimplePerson.class);
	}

	@Data
	static class SimplePerson {

		@Id String id;
	}

	@Data
	static class VersionedPerson {

		@Id String id;
		@Version long version;
	}
}

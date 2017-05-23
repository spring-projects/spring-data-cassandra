/*
 * Copyright 2017 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import lombok.Data;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.core.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;

/**
 * Unit tests for {@link SimpleCassandraRepository}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class SimpleCassandraRepositoryUnitTests {

	BasicCassandraMappingContext mappingContext = new BasicCassandraMappingContext();
	MappingCassandraConverter converter = new MappingCassandraConverter(mappingContext);

	SimpleCassandraRepository<Object, ? extends Serializable> repository;

	@Mock CassandraOperations cassandraOperations;
	@Mock UserTypeResolver userTypeResolver;

	@Before
	public void before() {
		mappingContext.setUserTypeResolver(userTypeResolver);
	}

	@Test // DATACASS-428
	public void saveShouldInsertNewPrimaryKeyOnlyEntity() {

		CassandraPersistentEntity<?> entity = converter.getMappingContext().getRequiredPersistentEntity(SimplePerson.class);

		repository = new SimpleCassandraRepository<Object, String>(new MappingCassandraEntityInformation(entity, converter),
				cassandraOperations);

		SimplePerson person = new SimplePerson();

		when(cassandraOperations.insert(person)).thenReturn(person);

		Object result = repository.save(person);

		assertThat(result).isEqualTo(person);
		verify(cassandraOperations).insert(person);
	}

	@Test // DATACASS-428
	public void saveShouldUpdateNewEntity() {

		CassandraPersistentEntity<?> entity = converter.getMappingContext().getRequiredPersistentEntity(Person.class);

		repository = new SimpleCassandraRepository<Object, String>(new MappingCassandraEntityInformation(entity, converter),
				cassandraOperations);

		Person person = new Person();

		when(cassandraOperations.update(person)).thenReturn(person);

		Object result = repository.save(person);

		assertThat(result).isEqualTo(person);
		verify(cassandraOperations).update(person);
	}

	@Test // DATACASS-428
	public void saveShouldUpdateExistingEntity() {

		CassandraPersistentEntity<?> entity = converter.getMappingContext().getRequiredPersistentEntity(Person.class);

		repository = new SimpleCassandraRepository<Object, String>(new MappingCassandraEntityInformation(entity, converter),
				cassandraOperations);

		Person person = new Person();
		person.setFirstname("foo");
		person.setLastname("bar");

		when(cassandraOperations.update(person)).thenReturn(person);

		Object result = repository.save(person);

		assertThat(result).isEqualTo(person);
		verify(cassandraOperations).update(person);
	}

	@Test // DATACASS-428
	public void insertShouldInsertEntity() {

		CassandraPersistentEntity<?> entity = converter.getMappingContext().getRequiredPersistentEntity(Person.class);

		repository = new SimpleCassandraRepository<Object, String>(new MappingCassandraEntityInformation(entity, converter),
				cassandraOperations);

		Person person = new Person();

		when(cassandraOperations.insert(person)).thenReturn(person);

		Object result = repository.insert(person);

		assertThat(result).isEqualTo(person);
		verify(cassandraOperations).insert(person);
	}

	@Data
	static class SimplePerson {

		@Id String id;
	}
}

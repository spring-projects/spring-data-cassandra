/*
 * Copyright 2019 the original author or authors.
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

import static org.mockito.Mockito.*;

import lombok.Data;
import reactor.core.publisher.Mono;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.cassandra.core.EntityWriteResult;
import org.springframework.data.cassandra.core.InsertOptions;
import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;

import com.datastax.driver.core.UserType;
import com.datastax.driver.core.querybuilder.Insert;

/**
 * Unit tests for {@link SimpleReactiveCassandraRepository}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class SimpleReactiveCassandraRepositoryUnitTests {

	CassandraMappingContext mappingContext = new CassandraMappingContext();
	MappingCassandraConverter converter = new MappingCassandraConverter(mappingContext);

	SimpleReactiveCassandraRepository<Object, ? extends Serializable> repository;

	@Mock ReactiveCassandraOperations cassandraOperations;
	@Mock UserTypeResolver userTypeResolver;
	@Mock UserType userType;
	@Mock EntityWriteResult writeResult;

	@Captor ArgumentCaptor<Insert> insertCaptor;

	@Before
	public void before() {
		mappingContext.setUserTypeResolver(userTypeResolver);
		when(cassandraOperations.getConverter()).thenReturn(converter);
	}

	@Test // DATACASS-576
	public void shouldInsertNewVersionedEntity() {

		when(cassandraOperations.insert(any(), any(InsertOptions.class))).thenReturn(Mono.just(writeResult));

		CassandraPersistentEntity<?> entity = converter.getMappingContext()
				.getRequiredPersistentEntity(VersionedPerson.class);

		repository = new SimpleReactiveCassandraRepository<Object, String>(
				new MappingCassandraEntityInformation(entity, converter), cassandraOperations);

		VersionedPerson versionedPerson = new VersionedPerson();

		repository.save(versionedPerson);

		verify(cassandraOperations).insert(versionedPerson, InsertOptions.builder().withInsertNulls().build());
	}

	@Test // DATACASS-576
	public void shouldUpdateExistingVersionedEntity() {

		CassandraPersistentEntity<?> entity = converter.getMappingContext()
				.getRequiredPersistentEntity(VersionedPerson.class);

		repository = new SimpleReactiveCassandraRepository<Object, String>(
				new MappingCassandraEntityInformation(entity, converter), cassandraOperations);

		VersionedPerson versionedPerson = new VersionedPerson();
		versionedPerson.setVersion(2);

		repository.save(versionedPerson);

		verify(cassandraOperations).update(versionedPerson);
	}

	@Data
	static class VersionedPerson {

		@Id String id;
		@Version long version;
	}

}

/*
 * Copyright 2019-2021 the original author or authors.
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
import reactor.core.publisher.Mono;

import java.io.Serializable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.cassandra.core.EntityWriteResult;
import org.springframework.data.cassandra.core.InsertOptions;
import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;

import com.datastax.oss.driver.api.core.type.UserDefinedType;

/**
 * Unit tests for {@link SimpleReactiveCassandraRepository}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
class SimpleReactiveCassandraRepositoryUnitTests {

	private CassandraMappingContext mappingContext = new CassandraMappingContext();
	private MappingCassandraConverter converter = new MappingCassandraConverter(mappingContext);

	private SimpleReactiveCassandraRepository<Object, ? extends Serializable> repository;

	@Mock ReactiveCassandraOperations cassandraOperations;
	@Mock UserTypeResolver userTypeResolver;
	@Mock UserDefinedType userType;
	@Mock EntityWriteResult writeResult;

	@BeforeEach
	void before() {
		mappingContext.setUserTypeResolver(userTypeResolver);
		when(cassandraOperations.getConverter()).thenReturn(converter);
	}

	@Test // DATACASS-576
	void shouldInsertNewVersionedEntity() {

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
	void shouldUpdateExistingVersionedEntity() {

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

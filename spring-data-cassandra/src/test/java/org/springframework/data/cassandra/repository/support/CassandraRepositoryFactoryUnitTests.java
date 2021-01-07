/*
 * Copyright 2016-2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.Serializable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.repository.query.CassandraEntityInformation;
import org.springframework.data.repository.Repository;

/**
 * Unit tests for {@link CassandraRepositoryFactory}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@SuppressWarnings({ "rawtypes", "unchecked" })
public class CassandraRepositoryFactoryUnitTests {

	@Mock BasicCassandraPersistentEntity entity;
	@Mock CassandraConverter converter;
	@Mock CassandraMappingContext mappingContext;
	@Mock CassandraTemplate template;

	@BeforeEach
	void setUp() {

		when(template.getConverter()).thenReturn(converter);
		when(converter.getMappingContext()).thenReturn(mappingContext);
	}

	@Test // DATACASS-7
	void usesMappingCassandraEntityInformationIfMappingContextSet() {

		when(mappingContext.getRequiredPersistentEntity(Person.class)).thenReturn(entity);

		CassandraRepositoryFactory repositoryFactory = new CassandraRepositoryFactory(template);

		CassandraEntityInformation<Person, Serializable> entityInformation = repositoryFactory
				.getEntityInformation(Person.class);

		assertThat(entityInformation).isInstanceOf(MappingCassandraEntityInformation.class);
	}

	@Test // DATACASS-7
	void createsRepositoryWithIdTypeLong() {

		when(mappingContext.getRequiredPersistentEntity(Person.class)).thenReturn(entity);

		CassandraRepositoryFactory repositoryFactory = new CassandraRepositoryFactory(template);
		MyPersonRepository repository = repositoryFactory.getRepository(MyPersonRepository.class);

		assertThat(repository).isNotNull();
	}

	interface MyPersonRepository extends Repository<Person, Long> {}

}

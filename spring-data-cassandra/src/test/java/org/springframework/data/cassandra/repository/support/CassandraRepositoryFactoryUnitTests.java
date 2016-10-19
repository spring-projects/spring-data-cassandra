/*
 * Copyright 2016 the original author or authors.
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

import java.io.Serializable;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.repository.query.CassandraEntityInformation;
import org.springframework.data.repository.Repository;

/**
 * Unit tests for {@link CassandraRepositoryFactory}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings({ "rawtypes", "unchecked" })
public class CassandraRepositoryFactoryUnitTests {

	@Mock CassandraConverter converter;

	@Mock CassandraMappingContext mappingContext;

	@Mock CassandraPersistentEntity entity;

	@Mock CassandraTemplate template;

	@Before
	public void setUp() {
		when(template.getConverter()).thenReturn(converter);
		when(converter.getMappingContext()).thenReturn(mappingContext);
	}

	/**
	 * @see DATACASS-7
	 */
	@Test
	public void usesMappingCassandraEntityInformationIfMappingContextSet() {
		when(mappingContext.getPersistentEntity(Person.class)).thenReturn(entity);
		when(entity.getType()).thenReturn(Person.class);

		CassandraRepositoryFactory repositoryFactory = new CassandraRepositoryFactory(template);

		CassandraEntityInformation<Person, Serializable> entityInformation = repositoryFactory
				.getEntityInformation(Person.class);

		assertThat(entityInformation).isInstanceOf(MappingCassandraEntityInformation.class);
	}

	/**
	 * @see DATACASS-7
	 */
	@Test
	public void createsRepositoryWithIdTypeLong() {
		when(mappingContext.getPersistentEntity(Person.class)).thenReturn(entity);
		when(entity.getType()).thenReturn(Person.class);

		CassandraRepositoryFactory repositoryFactory = new CassandraRepositoryFactory(template);
		MyPersonRepository repository = repositoryFactory.getRepository(MyPersonRepository.class);

		assertThat(repository).isNotNull();
	}

	interface MyPersonRepository extends Repository<Person, Long> {}
}

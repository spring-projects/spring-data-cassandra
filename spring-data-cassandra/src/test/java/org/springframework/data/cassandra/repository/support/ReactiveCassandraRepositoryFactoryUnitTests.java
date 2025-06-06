/*
 * Copyright 2016-2025 the original author or authors.
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.support.AbstractRepositoryMetadata;

/**
 * Unit tests for {@link ReactiveCassandraRepositoryFactory}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@SuppressWarnings({ "rawtypes", "unchecked" })
public class ReactiveCassandraRepositoryFactoryUnitTests {

	@Mock BasicCassandraPersistentEntity entity;
	@Mock CassandraConverter converter;
	@Mock CassandraMappingContext mappingContext;
	@Mock ReactiveCassandraTemplate template;

	@BeforeEach
	void setUp() {

		when(template.getConverter()).thenReturn(converter);
		when(converter.getProjectionFactory()).thenReturn(new SpelAwareProxyProjectionFactory());
		when(converter.getMappingContext()).thenReturn(mappingContext);
	}

	@Test // DATACASS-335
	void usesMappingCassandraEntityInformationIfMappingContextSet() {

		when(mappingContext.getRequiredPersistentEntity(Person.class)).thenReturn(entity);

		ReactiveCassandraRepositoryFactory repositoryFactory = new ReactiveCassandraRepositoryFactory(template);

		EntityInformation<?, ?> entityInformation = repositoryFactory
				.getEntityInformation(AbstractRepositoryMetadata.getMetadata(MyPersonRepository.class));

		assertThat(entityInformation).isInstanceOf(MappingCassandraEntityInformation.class);
	}

	@Test // DATACASS-335
	void createsRepositoryWithIdTypeLong() {

		when(mappingContext.getRequiredPersistentEntity(Person.class)).thenReturn(entity);

		ReactiveCassandraRepositoryFactory repositoryFactory = new ReactiveCassandraRepositoryFactory(template);
		MyPersonRepository repository = repositoryFactory.getRepository(MyPersonRepository.class);

		assertThat(repository).isNotNull();
	}

	interface MyPersonRepository extends Repository<Person, Long> {}

}

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
package org.springframework.data.cassandra.repository.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.core.ReactiveSession;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Unit tests for {@link ReactiveCassandraRepositoriesRegistrar}.
 *
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class ReactiveCassandraRepositoriesRegistrarUnitTests {

	@Configuration
	@EnableReactiveCassandraRepositories(basePackages = "org.springframework.data.cassandra.repository.config",
			considerNestedRepositories = true,
			includeFilters = @Filter(pattern = ".*ReactivePersonRepository", type = FilterType.REGEX))
	static class Config {

		@Bean
		public ReactiveCassandraTemplate reactiveCassandraTemplate() throws Exception {
			return new ReactiveCassandraTemplate(mock(ReactiveSession.class), new MappingCassandraConverter());
		}
	}

	@Autowired ReactivePersonRepository personRepository;
	@Autowired ApplicationContext context;

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void testConfiguration() {}

	static interface ReactivePersonRepository extends ReactiveCassandraRepository<Person, String> {}
}

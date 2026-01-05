/*
 * Copyright 2016-present the original author or authors.
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
package org.springframework.data.cassandra.repository.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationBeanNameGenerator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Unit tests for {@link ReactiveCassandraRepositoriesRegistrar}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@SpringJUnitConfig
public class ReactiveCassandraRepositoriesRegistrarUnitTests {

	@Configuration
	@EnableReactiveCassandraRepositories(basePackages = "org.springframework.data.cassandra.repository.config",
			considerNestedRepositories = true,
			nameGenerator = MyBeanNameGenerator.class,
			includeFilters = @Filter(pattern = ".*ReactivePersonRepository", type = FilterType.REGEX))
	static class Config {

		@Bean
		public ReactiveCassandraTemplate reactiveCassandraTemplate() throws Exception {

			CassandraMappingContext mappingContext = new CassandraMappingContext();
			mappingContext.setUserTypeResolver(mock(UserTypeResolver.class));
			MappingCassandraConverter converter = new MappingCassandraConverter(mappingContext);

			return new ReactiveCassandraTemplate(mock(ReactiveSession.class), converter);
		}
	}

	@Autowired ApplicationContext context;
	@Autowired ReactivePersonRepository personRepository;

	@Test // DATACASS-335, GH-1509
	void testConfiguration() {
		assertThat(context.containsBean("reactiveCassandraRepositoriesRegistrarUnitTests.ReactivePersonREPO")).isTrue();
	}

	interface ReactivePersonRepository extends ReactiveCassandraRepository<Person, String> {}

	static class MyBeanNameGenerator extends AnnotationBeanNameGenerator {

		@Override
		public String generateBeanName(BeanDefinition definition, BeanDefinitionRegistry registry) {
			return super.generateBeanName(definition, registry).replaceAll("Repository", "REPO");
		}
	}
}

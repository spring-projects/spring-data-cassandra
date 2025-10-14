/*
 * Copyright 2025-present the original author or authors.
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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.DefaultBeanNameGenerator;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.data.cassandra.repository.aot.TestCassandraAotRepositoryContext;
import org.springframework.data.cassandra.repository.config.CassandraRepositoryConfigurationExtension.CassandraRepositoryRegistrationAotProcessor;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.core.support.RepositoryComposition;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

/**
 * @author Christoph Strobl
 */
class CassandraRepositoryRegistrationAotProcessorUnitTests {

	@Test // GH-1611
	void shouldNotAttemptToProcessReactiveRepository() {

		AnnotationRepositoryConfigurationSource configSource = new AnnotationRepositoryConfigurationSource(
			AnnotationMetadata.introspect(ReactiveCassandraRepositoryConfiguration.class), EnableReactiveCassandraRepositories.class, new DefaultResourceLoader(),
			new StandardEnvironment(), Mockito.mock(BeanDefinitionRegistry.class), DefaultBeanNameGenerator.INSTANCE);

		TestCassandraAotRepositoryContext<?> context = new TestCassandraAotRepositoryContext<>(new DefaultListableBeanFactory(), ReactiveCassandraRepo.class, RepositoryComposition.empty(), configSource);

		CassandraRepositoryRegistrationAotProcessor processor = new CassandraRepositoryRegistrationAotProcessor();
		assertThat(processor.contributeAotRepository(context)).isNull();
	}

	interface ReactiveCassandraRepo extends ReactiveCrudRepository<SampleEntity, Long> {

	}

	static class SampleEntity {

	}

	@EnableReactiveCassandraRepositories
	static class ReactiveCassandraRepositoryConfiguration {

	}
}

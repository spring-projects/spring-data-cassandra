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
package org.springframework.data.cassandra.repository.config;

import static org.assertj.core.api.Assertions.*;

import java.util.Collection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.type.StandardAnnotationMetadata;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.repository.MapIdCassandraRepository;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryConfiguration;
import org.springframework.data.repository.config.RepositoryConfigurationSource;

/**
 * Unit tests for {@link CassandraRepositoryConfigurationExtension}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class CassandraRepositoryConfigurationExtensionUnitTests {

	private StandardAnnotationMetadata metadata = new StandardAnnotationMetadata(Config.class, true);
	private ResourceLoader loader = new PathMatchingResourcePatternResolver();
	private Environment environment = new StandardEnvironment();
	private BeanDefinitionRegistry registry = new DefaultListableBeanFactory();
	private RepositoryConfigurationSource configurationSource = new AnnotationRepositoryConfigurationSource(metadata,
			EnableCassandraRepositories.class, loader, environment, registry);

	private CassandraRepositoryConfigurationExtension extension;

	@BeforeEach
	void setUp() {
		extension = new CassandraRepositoryConfigurationExtension();
	}

	@Test // DATACASS-257
	void isStrictMatchIfDomainTypeIsAnnotatedWithTable() {
		assertHasRepo(SampleRepository.class, extension.getRepositoryConfigurations(configurationSource, loader, true));
	}

	@Test // DATACASS-257
	void isStrictMatchIfRepositoryExtendsStoreSpecificBase() {
		assertHasRepo(StoreRepository.class, extension.getRepositoryConfigurations(configurationSource, loader, true));
	}

	@Test // DATACASS-257
	void isNotStrictMatchIfDomainTypeIsNotAnnotatedWithTable() {

		assertDoesNotHaveRepo(UnannotatedRepository.class,
				extension.getRepositoryConfigurations(configurationSource, loader, true));
	}

	private static void assertDoesNotHaveRepo(Class<?> repositoryInterface,
			Collection<RepositoryConfiguration<RepositoryConfigurationSource>> configs) {

		try {

			assertHasRepo(repositoryInterface, configs);
			fail("Expected not to find config for repository interface ".concat(repositoryInterface.getName()));
		} catch (AssertionError error) {
			// repo not there. we're fine.
		}
	}

	private static void assertHasRepo(Class<?> repositoryInterface,
			Collection<RepositoryConfiguration<RepositoryConfigurationSource>> configs) {

		for (RepositoryConfiguration<?> config : configs) {
			if (config.getRepositoryInterface().equals(repositoryInterface.getName())) {
				return;
			}
		}

		fail(String.format("Expected to find config for repository interface %s but got %s", repositoryInterface.getName(),
				configs.toString()));
	}

	@EnableCassandraRepositories(considerNestedRepositories = true)
	private static class Config {

	}

	@Table
	private static class Sample {}

	interface SampleRepository extends Repository<Sample, Long> {}

	interface UnannotatedRepository extends Repository<Object, Long> {}

	interface StoreRepository extends MapIdCassandraRepository<Object> {}
}

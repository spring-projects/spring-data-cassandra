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

import static org.junit.Assert.*;

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
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryConfiguration;
import org.springframework.data.repository.config.RepositoryConfigurationSource;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.data.repository.reactive.RxJava2CrudRepository;

/**
 * Unit tests for {@link ReactiveCassandraRepositoryConfigurationExtension}.
 *
 * @author Mark Paluch
 */
public class ReactiveCassandraRepositoryConfigurationExtensionUnitTests {

	private StandardAnnotationMetadata metadata = new StandardAnnotationMetadata(Config.class, true);
	private ResourceLoader loader = new PathMatchingResourcePatternResolver();
	private Environment environment = new StandardEnvironment();
	private BeanDefinitionRegistry registry = new DefaultListableBeanFactory();
	private RepositoryConfigurationSource configurationSource = new AnnotationRepositoryConfigurationSource(metadata,
			EnableReactiveCassandraRepositories.class, loader, environment, registry);

	private ReactiveCassandraRepositoryConfigurationExtension extension;

	@BeforeEach
	void setUp() {
		extension = new ReactiveCassandraRepositoryConfigurationExtension();
	}

	@Test // DATACASS-335
	void isStrictMatchIfDomainTypeIsAnnotatedWithTable() {
		assertHasRepo(SampleRepository.class, extension.getRepositoryConfigurations(configurationSource, loader, true));
	}

	@Test // DATACASS-335
	void isStrictMatchIfRepositoryExtendsStoreSpecificBase() {
		assertHasRepo(StoreRepository.class, extension.getRepositoryConfigurations(configurationSource, loader, true));
	}

	@Test // DATACASS-335
	void isNotStrictMatchIfDomainTypeIsNotAnnotatedWithDocument() {

		assertDoesNotHaveRepo(UnannotatedRepository.class,
				extension.getRepositoryConfigurations(configurationSource, loader, true));
	}

	private static void assertDoesNotHaveRepo(Class<?> repositoryInterface,
			Collection<RepositoryConfiguration<RepositoryConfigurationSource>> configs) {

		try {

			assertHasRepo(repositoryInterface, configs);
			fail("Expected not to find config for repository interface " + repositoryInterface.getName());
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

	@EnableReactiveCassandraRepositories(considerNestedRepositories = true)
	private static class Config {

	}

	@Table
	private static class Sample {}

	interface SampleRepository extends RxJava2CrudRepository<Sample, Long> {}

	interface UnannotatedRepository extends ReactiveCrudRepository<Object, Long> {}

	interface StoreRepository extends ReactiveCassandraRepository<Object, Long> {}
}

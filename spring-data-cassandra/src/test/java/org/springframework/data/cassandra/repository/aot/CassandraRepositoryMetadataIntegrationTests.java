/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.cassandra.repository.aot;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Integration tests for the {@link PersonRepository} JSON metadata via {@link CassandraRepositoryContributor}.
 *
 * @author Mark Paluch
 */
@SpringJUnitConfig(
		classes = CassandraRepositoryMetadataIntegrationTests.CassandraRepositoryContributorConfiguration.class)
class CassandraRepositoryMetadataIntegrationTests {

	@Autowired AbstractApplicationContext context;

	@Configuration
	@EnableCassandraRepositories(considerNestedRepositories = true,
			includeFilters = { @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, value = Person.class) })
	static class CassandraRepositoryContributorConfiguration extends AotFragmentTestConfigurationSupport {

		public CassandraRepositoryContributorConfiguration() {
			super(PersonRepository.class, CassandraRepositoryContributorConfiguration.class);
		}

		@Bean
		public CassandraTemplate cassandraTemplate() {
			return mock(CassandraTemplate.class);
		}
	}

	@Test // GH-1566
	void shouldDocumentBase() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).isObject() //
				.containsEntry("name", PersonRepository.class.getName()) //
				.containsEntry("module", "Cassandra") //
				.containsEntry("type", "IMPERATIVE");
	}

	@Test // GH-1566
	void shouldDocumentDerivedQuery() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).inPath("$.methods[?(@.name == 'countByLastname')].query").isArray().first().isObject()
				.containsEntry("query", "SELECT count(1) FROM person WHERE lastname=?");
	}

	@Test // GH-1566
	void shouldDocumentDeclaredQuery() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).inPath("$.methods[?(@.name == 'findDeclaredByFirstname')].query").isArray().first().isObject()
				.containsEntry("query", "select * from person where firstname = ?");
	}

	@Test // GH-1566
	void shouldDocumentNamedQuery() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).inPath("$.methods[?(@.name == 'findNamedByFirstname')].query").isArray().first().isObject()
				.containsEntry("query", "SELECT * FROM person WHERE firstname=? ALLOW FILTERING")
				.containsEntry("name", "Person.findNamedByFirstname");
	}

	@Test // GH-1566
	void shouldNotIncludeVectorSearch() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).inPath("$.methods[?(@.name == 'findAllByVector')].query").isArray().isEmpty();
	}

	@Test // GH-1566
	void shouldDocumentBaseFragment() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).inPath("$.methods[?(@.name == 'existsById')].fragment").isArray().first().isObject()
				.containsEntry("fragment", "org.springframework.data.cassandra.repository.support.SimpleCassandraRepository");
	}

	private Resource getResource() {

		String location = PersonRepository.class.getPackageName().replace('.', '/') + "/"
				+ PersonRepository.class.getSimpleName() + ".json";
		return new UrlResource(context.getBeanFactory().getBeanClassLoader().getResource(location));
	}

}

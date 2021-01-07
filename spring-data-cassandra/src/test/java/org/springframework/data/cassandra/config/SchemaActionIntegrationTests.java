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

package org.springframework.data.cassandra.config;

import static org.assertj.core.api.Assertions.*;

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.data.cassandra.core.cql.SessionCallback;
import org.springframework.data.cassandra.core.cql.session.init.KeyspacePopulator;
import org.springframework.data.cassandra.core.cql.session.init.ResourceKeyspacePopulator;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;
import org.springframework.data.cassandra.test.util.IntegrationTestsSupport;
import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

/**
 * Integration test testing various {@link SchemaAction SchemaActions} on startup of a Spring configured, Apache
 * Cassandra application client.
 *
 * @author John Blum
 * @author Mark Paluch
 * @see AbstractKeyspaceCreatingIntegrationTests
 */
class SchemaActionIntegrationTests extends IntegrationTestsSupport {

	private static final String CREATE_PERSON_TABLE_CQL = "CREATE TABLE IF NOT EXISTS person (id int, firstName text, lastName text, PRIMARY KEY(id));";

	ConfigurableApplicationContext newApplicationContext(Class<?>... annotatedClasses) {

		AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext(annotatedClasses);

		applicationContext.registerShutdownHook();

		return applicationContext;
	}

	@SuppressWarnings("all")
	protected <T> T doInSessionWithConfiguration(Class<?> annotatedClass, SessionCallback<T> sessionCallback) {

		try (ConfigurableApplicationContext applicationContext = newApplicationContext(annotatedClass)) {
			return sessionCallback.doInSession(applicationContext.getBean(CqlSession.class));
		}
	}

	@SuppressWarnings("all")
	protected void assertTableWithColumnsExists(CqlSession session, String tableName, String... columns) {

		KeyspaceMetadata keyspaceMetadata = session.refreshSchema().getKeyspace(session.getKeyspace().get()).orElse(null);

		assertThat(keyspaceMetadata).isNotNull();

		TableMetadata tableMetadata = keyspaceMetadata.getTable(tableName).orElse(null);

		assertThat(tableMetadata).isNotNull();
		assertThat(tableMetadata.getColumns()).hasSize(columns.length);

		for (String columnName : columns) {
			assertThat(tableMetadata.getColumn(columnName)).isNotNull();
		}

		assertThat(tableMetadata.getColumns()).hasSize(columns.length);
	}

	@Test
	void createWithNoExistingTableCreatesTableFromEntity() {

		doInSessionWithConfiguration(CreateWithNoExistingTableConfiguration.class, session -> {

			assertTableWithColumnsExists(session, "person", "firstName", "lastName", "nickname", "birthDate",
					"numberOfChildren", "cool", "createdDate", "zoneId", "mainAddress", "alternativeAddresses");

			return null;
		});
	}

	@Test
	void createWithExistingTableThrowsErrorWhenCreatingTableFromEntity() {

		try {

			doInSessionWithConfiguration(CreateWithExistingTableConfiguration.class, session -> {

				fail(String.format("%s should have failed", CreateWithExistingTableConfiguration.class.getSimpleName()));
				return null;
			});

			fail("Expected BeanCreationException");

		} catch (BeanCreationException cause) {
			assertThat(cause).hasMessageContaining("person already exists");
		}
	}

	@Test
	void createIfNotExistsWithNoExistingTableCreatesTableFromEntity() {

		doInSessionWithConfiguration(CreateIfNotExistsWithNoExistingTableConfiguration.class, session -> {

			assertTableWithColumnsExists(session, "person", "firstName", "lastName", "nickname", "birthDate",
					"numberOfChildren", "cool", "createdDate", "zoneId", "mainAddress", "alternativeAddresses");

			return null;
		});
	}

	@Test
	void createIfNotExistsWithExistingTableUsesExistingTable() {

		doInSessionWithConfiguration(CreateIfNotExistsWithExistingTableConfiguration.class, session -> {

			assertTableWithColumnsExists(session, "person", "id", "firstName", "lastName");

			return null;
		});
	}

	@Test
	void recreateTableFromEntityDropsExistingTable() {

		doInSessionWithConfiguration(RecreateSchemaActionWithExistingTableConfiguration.class, session -> {

			assertTableWithColumnsExists(session, "person", "firstName", "lastName", "nickname", "birthDate",
					"numberOfChildren", "cool", "createdDate", "zoneId", "mainAddress", "alternativeAddresses");

			return null;
		});
	}

	@Configuration
	static class CreateWithNoExistingTableConfiguration extends IntegrationTestConfig {

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.CREATE;
		}

		@Override
		protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
			return Collections.singleton(Person.class);
		}
	}

	@Configuration
	static class CreateWithExistingTableConfiguration extends IntegrationTestConfig {

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.CREATE;
		}

		@Nullable
		@Override
		protected KeyspacePopulator keyspacePopulator() {
			return new ResourceKeyspacePopulator(new ByteArrayResource(CREATE_PERSON_TABLE_CQL.getBytes()));
		}

		@Override
		protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
			return Collections.singleton(Person.class);
		}
	}

	@Configuration
	static class CreateIfNotExistsWithNoExistingTableConfiguration extends IntegrationTestConfig {

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.CREATE_IF_NOT_EXISTS;
		}

		@Override
		protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
			return Collections.singleton(Person.class);
		}
	}

	@Configuration
	static class CreateIfNotExistsWithExistingTableConfiguration extends IntegrationTestConfig {

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.CREATE_IF_NOT_EXISTS;
		}

		@Nullable
		@Override
		protected KeyspacePopulator keyspacePopulator() {
			return new ResourceKeyspacePopulator(new ByteArrayResource(CREATE_PERSON_TABLE_CQL.getBytes()));
		}

		@Override
		protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
			return Collections.singleton(Person.class);
		}
	}

	@Configuration
	static class RecreateSchemaActionWithExistingTableConfiguration extends IntegrationTestConfig {

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.RECREATE;
		}

		@Nullable
		@Override
		protected KeyspacePopulator keyspacePopulator() {
			return new ResourceKeyspacePopulator(new ByteArrayResource(CREATE_PERSON_TABLE_CQL.getBytes()));
		}

		@Override
		protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
			return Collections.singleton(Person.class);
		}
	}
}

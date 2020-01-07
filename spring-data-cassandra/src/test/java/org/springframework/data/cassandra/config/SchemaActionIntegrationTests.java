/*
 * Copyright 2016-2020 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.cql.SessionCallback;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

/**
 * Integration test testing various {@link SchemaAction SchemaActions} on startup of a Spring configured,
 * Apache Cassandra application client.
 *
 * @author John Blum
 * @author Mark Paluch
 * @see org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest
 */
public class SchemaActionIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	protected static final String CREATE_PERSON_TABLE_CQL =
			"CREATE TABLE IF NOT EXISTS person (id int, firstName text, lastName text, PRIMARY KEY(id));";

	protected static final String DROP_ADDRESS_TYPE_CQL = "DROP TYPE IF EXISTS address";
	protected static final String DROP_PERSON_TABLE_CQL = "DROP TABLE IF EXISTS person";

	protected ConfigurableApplicationContext newApplicationContext(Class<?>... annotatedClasses) {

		AnnotationConfigApplicationContext applicationContext =
				new AnnotationConfigApplicationContext(annotatedClasses);

		applicationContext.registerShutdownHook();

		return applicationContext;
	}

	@SuppressWarnings("all")
	protected <T> T doInSessionWithConfiguration(Class<?> annotatedClass, SessionCallback<T> sessionCallback) {

		try (ConfigurableApplicationContext applicationContext = newApplicationContext(annotatedClass)) {
			return sessionCallback.doInSession(applicationContext.getBean(Session.class));
		}
	}

	@SuppressWarnings("all")
	protected void assertTableWithColumnsExists(Session session, String tableName, String... columns) {

		Metadata clusterMetadata = session.getCluster().getMetadata();
		KeyspaceMetadata keyspaceMetadata = clusterMetadata.getKeyspace(getKeyspace());

		assertThat(keyspaceMetadata).isNotNull();

		TableMetadata tableMetadata = keyspaceMetadata.getTable(tableName);

		assertThat(tableMetadata).isNotNull();
		assertThat(tableMetadata.getColumns()).hasSize(columns.length);

		for (String columnName : columns) {
			assertThat(tableMetadata.getColumn(columnName)).isNotNull();
		}

		assertThat(tableMetadata.getColumns()).hasSize(columns.length);
	}

	@Before
	public void setup() {

		Session session = getSession();

		session.execute(DROP_PERSON_TABLE_CQL);
		session.execute(DROP_ADDRESS_TYPE_CQL);
	}

	@Test
	public void createWithNoExistingTableCreatesTableFromEntity() {

		doInSessionWithConfiguration(CreateWithNoExistingTableConfiguration.class, session -> {

			assertTableWithColumnsExists(session, "person", "firstName", "lastName", "nickname",
					"birthDate", "numberOfChildren", "cool", "createdDate", "zoneId", "mainAddress",
					"alternativeAddresses");

			return null;
		});
	}

	@Test
	public void createWithExistingTableThrowsErrorWhenCreatingTableFromEntity() {

		try {

			doInSessionWithConfiguration(CreateWithExistingTableConfiguration.class, session -> {
				fail(String.format("%s should have failed", CreateWithExistingTableConfiguration.class.getSimpleName()));
				return null;
			});

			fail("Expected BeanCreationException");

		} catch (BeanCreationException cause) {
			assertThat(cause).hasMessageContaining(String.format("Table %s.person already exists", getKeyspace()));
		}
	}

	@Test
	public void createIfNotExistsWithNoExistingTableCreatesTableFromEntity() {

		doInSessionWithConfiguration(CreateIfNotExistsWithNoExistingTableConfiguration.class, session -> {

			assertTableWithColumnsExists(session, "person", "firstName", "lastName", "nickname",
				"birthDate", "numberOfChildren", "cool", "createdDate", "zoneId", "mainAddress",
				"alternativeAddresses");

			return null;
		});
	}

	@Test
	public void createIfNotExistsWithExistingTableUsesExistingTable() {

		doInSessionWithConfiguration(CreateIfNotExistsWithExistingTableConfiguration.class, session -> {

			assertTableWithColumnsExists(session, "person", "id", "firstName", "lastName");

			return null;
		});
	}

	@Test
	public void recreateTableFromEntityDropsExistingTable() {

		doInSessionWithConfiguration(RecreateSchemaActionWithExistingTableConfiguration.class, session -> {

			assertTableWithColumnsExists(session, "person", "firstName", "lastName", "nickname",
				"birthDate", "numberOfChildren", "cool", "createdDate", "zoneId", "mainAddress",
				"alternativeAddresses");

			return null;
		});
	}

	@Configuration
	static class CreateWithNoExistingTableConfiguration extends CassandraConfiguration {

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.CREATE;
		}
	}

	@Configuration
	static class CreateWithExistingTableConfiguration extends CassandraConfiguration {

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.CREATE;
		}

		@Override
		protected List<String> getStartupScripts() {
			return Collections.singletonList(CREATE_PERSON_TABLE_CQL);
		}
	}

	@Configuration
	static class CreateIfNotExistsWithNoExistingTableConfiguration extends CassandraConfiguration {

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.CREATE_IF_NOT_EXISTS;
		}
	}

	@Configuration
	static class CreateIfNotExistsWithExistingTableConfiguration extends CassandraConfiguration {

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.CREATE_IF_NOT_EXISTS;
		}

		@Override
		protected List<String> getStartupScripts() {
			return Collections.singletonList(CREATE_PERSON_TABLE_CQL);
		}
	}

	@Configuration
	static class RecreateSchemaActionWithExistingTableConfiguration extends CassandraConfiguration {

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.RECREATE;
		}

		@Override
		protected List<String> getStartupScripts() {
			return Collections.singletonList(CREATE_PERSON_TABLE_CQL);
		}
	}

	@Configuration
	static abstract class CassandraConfiguration extends AbstractCassandraConfiguration {

		@Bean
		@Override
		public CassandraClusterFactoryBean cluster() {

			return new CassandraClusterFactoryBean() {

				@Override
				public void afterPropertiesSet() throws Exception {
					// avoid Cassandra Cluster creation; use embedded
				}

				@Override
				public Cluster getObject() {
					return cassandraEnvironment.getCluster();
				}
			};
		}

		@Override
		protected Set<Class<?>> getInitialEntitySet() {
			return Collections.singleton(Person.class);
		}

		@Override
		protected String getKeyspaceName() {
			return keyspaceRule.getKeyspaceName();
		}
	}
}

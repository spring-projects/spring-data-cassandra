/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.data.cassandra.config;

import static org.assertj.core.api.Assertions.*;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.cql.SessionCallback;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.test.util.AbstractEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.util.KeyspaceRule;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

/**
 * The SchemaActionIntegrationTests class is a test suite of test cases testing the contract and behavior of various
 * {@link SchemaAction}s on startup of a Spring configured, Cassandra application client.
 *
 * @author John Blum
 * @author Mark Paluch
 */
public class SchemaActionIntegrationTests extends AbstractEmbeddedCassandraIntegrationTest {

	protected static final String KEYSPACE_NAME = SchemaActionIntegrationTests.class.getSimpleName().toLowerCase();

	protected static final String PERSON_TABLE_DEFINITION_CQL = String
			.format("CREATE TABLE %s.person (id int, firstName text, lastName text, PRIMARY KEY(id));", KEYSPACE_NAME);

	@Rule public ExpectedException exception = ExpectedException.none();

	@Rule public KeyspaceRule KEYSPACE_RULE = new KeyspaceRule(cassandraEnvironment, KEYSPACE_NAME);

	protected ConfigurableApplicationContext newApplicationContext(Class<?>... annotatedClasses) {
		AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext(annotatedClasses);

		applicationContext.registerShutdownHook();

		return applicationContext;
	}

	protected <T> T doInSessionWithConfiguration(Class<?> annotatedClass, SessionCallback<T> sessionCallback) {
		ConfigurableApplicationContext applicationContext = null;

		try {
			applicationContext = newApplicationContext(annotatedClass);
			return sessionCallback.doInSession(applicationContext.getBean(Session.class));
		} finally {
			if (applicationContext != null) {
				applicationContext.close();
			}
		}
	}

	protected void assertHasTableWithColumns(Session session, String tableName, String... columns) {

		Metadata clusterMetadata = session.getCluster().getMetadata();
		KeyspaceMetadata keyspaceMetadata = clusterMetadata.getKeyspace(KEYSPACE_NAME);

		assertThat(keyspaceMetadata).isNotNull();

		TableMetadata tableMetadata = keyspaceMetadata.getTable(tableName);

		assertThat(tableMetadata).isNotNull();
		assertThat(tableMetadata.getColumns()).hasSize(columns.length);

		for (String columnName : columns) {
			assertThat(tableMetadata.getColumn(columnName)).isNotNull();
		}

		assertThat(tableMetadata.getColumns()).hasSize(columns.length);
	}

	@Test
	public void createWithNoExistingTableCreatesTableFromEntity() {
		doInSessionWithConfiguration(CreateWithNoExistingTableConfiguration.class, (SessionCallback<Void>) session -> {
			assertHasTableWithColumns(session, "person", "firstName", "lastName", "nickname", "birthDate", "numberOfChildren",
					"cool", "createdDate", "zoneId", "mainAddress", "alternativeAddresses");
			return null;
		});
	}

	@Test
	public void createWithExistingTableThrowsErrorWhenCreatingTableFromEntity() {

		try {
			doInSessionWithConfiguration(CreateWithExistingTableConfiguration.class, s -> {
				fail(String.format("%s should have failed!", CreateWithExistingTableConfiguration.class.getSimpleName()));
				return null;
			});
			fail("Missing BeanCreationException");
		} catch (BeanCreationException e) {
			assertThat(e).hasMessageContaining(String.format("Table %s.person already exists", KEYSPACE_NAME));
		}
	}

	@Test
	public void createIfNotExistsWithNoExistingTableCreatesTableFromEntity() {
		doInSessionWithConfiguration(CreateIfNotExistsWithNoExistingTableConfiguration.class,
				(SessionCallback<Void>) session -> {
					assertHasTableWithColumns(session, "person", "firstName", "lastName", "nickname", "birthDate",
							"numberOfChildren", "cool", "createdDate", "zoneId", "mainAddress", "alternativeAddresses");
					return null;
				});
	}

	@Test
	public void createIfNotExistsWithExistingTableUsesExistingTable() {
		doInSessionWithConfiguration(CreateIfNotExistsWithExistingTableConfiguration.class,
				(SessionCallback<Void>) session -> {
					assertHasTableWithColumns(session, "person", "id", "firstName", "lastName");
					return null;
				});
	}

	@Test
	public void recreateTableFromEntityDropsExistingTable() {
		doInSessionWithConfiguration(RecreateSchemaActionWithExistingTableConfiguration.class,
				(SessionCallback<Void>) session -> {
					assertHasTableWithColumns(session, "person", "firstName", "lastName", "nickname", "birthDate",
							"numberOfChildren", "cool", "createdDate", "zoneId", "mainAddress", "alternativeAddresses");
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
			return Collections.singletonList(PERSON_TABLE_DEFINITION_CQL);
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
			return Collections.singletonList(PERSON_TABLE_DEFINITION_CQL);
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
			return Collections.singletonList(PERSON_TABLE_DEFINITION_CQL);
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
		protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
			return Collections.singleton(Person.class);
		}

		@Override
		protected String getKeyspaceName() {
			return KEYSPACE_NAME;
		}
	}
}

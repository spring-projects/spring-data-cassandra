/*
 * Copyright 2013-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.cassandra.config;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.*;
import static org.springframework.data.cassandra.config.CassandraSessionFactoryBean.*;

import java.util.Collections;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

/**
 * The CassandraSessionFactoryBeanUnitTests class is a test suite of test cases testing the contract and functionality
 * of the {@link CassandraSessionFactoryBean} class.
 *
 * @author John Blum
 * @see org.springframework.data.cassandra.config.CassandraSessionFactoryBean
 * @see <a href="https://jira.spring.io/browse/DATACASS-219>DATACASS-219</a>
 * @since 1.5.0
 */
@RunWith(MockitoJUnitRunner.class)
public class CassandraSessionFactoryBeanUnitTests {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Mock
	private CassandraConverter mockConverter;

	@Mock
	private Cluster mockCluster;

	@Mock
	private Session mockSession;

	private CassandraSessionFactoryBean factoryBean;

	@Before
	public void setup() {
		when(mockCluster.connect()).thenReturn(mockSession);
		when(mockSession.getCluster()).thenReturn(mockCluster);

		factoryBean = spy(new CassandraSessionFactoryBean());
		factoryBean.setCluster(mockCluster);
	}

	protected CqlIdentifier newCqlIdentifier(String id) {
		return new CqlIdentifier(id, false);
	}

	@Test
	public void afterPropertiesSetPerformsSchemaAction() throws Exception {
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
				assertThat(factoryBean.getSchemaAction(), is(equalTo(SchemaAction.RECREATE)));
				return null;
			}
		}).when(factoryBean).performSchemaAction();

		factoryBean.setConverter(mockConverter);
		factoryBean.setSchemaAction(SchemaAction.RECREATE);

		assertThat(factoryBean.getConverter(), is(equalTo(mockConverter)));
		assertThat(factoryBean.getSchemaAction(), is(equalTo(SchemaAction.RECREATE)));

		factoryBean.afterPropertiesSet();

		assertThat(factoryBean.getCassandraAdminOperations(), is(notNullValue(CassandraAdminOperations.class)));
		assertThat(factoryBean.getObject(), is(equalTo(mockSession)));

		verify(factoryBean, times(1)).performSchemaAction();
	}

	@Test
	public void afterPropertiesSetThrowsIllegalStateExceptionWhenConverterIsNull() throws Exception {
		exception.expect(IllegalStateException.class);
		exception.expectCause(is(nullValue(Throwable.class)));
		exception.expectMessage("Converter was not properly initialized");

		factoryBean.setCluster(mockCluster);
		factoryBean.afterPropertiesSet();
	}

	protected void performSchemaActionCallsCreateTableWithArgumentsMatchingTheSchemaAction(SchemaAction schemaAction,
			final boolean dropTables, final boolean dropUnused, final boolean ifNotExists) {

		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
				assertThat(invocationOnMock.getArgumentAt(0, Boolean.class), is(equalTo(dropTables)));
				assertThat(invocationOnMock.getArgumentAt(1, Boolean.class), is(equalTo(dropUnused)));
				assertThat(invocationOnMock.getArgumentAt(2, Boolean.class), is(equalTo(ifNotExists)));
				return null;
			}
		}).when(factoryBean).createTables(anyBoolean(), anyBoolean(), anyBoolean());

		factoryBean.setSchemaAction(schemaAction);

		assertThat(factoryBean.getSchemaAction(), is(equalTo(schemaAction)));

		factoryBean.performSchemaAction();

		verify(factoryBean, times(1)).createTables(eq(dropTables), eq(dropUnused), eq(ifNotExists));
	}

	@Test
	public void performsSchemaActionCreatesTablesWithDefaults() {
		performSchemaActionCallsCreateTableWithArgumentsMatchingTheSchemaAction(SchemaAction.CREATE,
			DEFAULT_DROP_TABLES, DEFAULT_DROP_UNUSED_TABLES, DEFAULT_CREATE_IF_NOT_EXISTS);
	}

	@Test
	public void performsSchemaActionCreatesTablesIfNotExists() {
		performSchemaActionCallsCreateTableWithArgumentsMatchingTheSchemaAction(SchemaAction.CREATE_IF_NOT_EXISTS,
			DEFAULT_DROP_TABLES, DEFAULT_DROP_UNUSED_TABLES, true);
	}

	@Test
	public void performsSchemaActionRecreatesTables() {
		performSchemaActionCallsCreateTableWithArgumentsMatchingTheSchemaAction(SchemaAction.RECREATE, true,
			DEFAULT_DROP_UNUSED_TABLES, DEFAULT_CREATE_IF_NOT_EXISTS);
	}

	@Test
	public void performsSchemaActionRecreatesAndDropsUnusedTables() {
		performSchemaActionCallsCreateTableWithArgumentsMatchingTheSchemaAction(SchemaAction.RECREATE_DROP_UNUSED,
			true, true, DEFAULT_CREATE_IF_NOT_EXISTS);
	}

	@Test
	public void performsSchemaActionDoesNotCallCreateTablesWhenSchemaActionIsNone() {
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
				fail("'createTables(..)' should not have been called");
				return null;
			}
		}).when(factoryBean).createTables(anyBoolean(), anyBoolean(), anyBoolean());

		factoryBean.setSchemaAction(SchemaAction.NONE);

		assertThat(factoryBean.getSchemaAction(), is(equalTo(SchemaAction.NONE)));

		factoryBean.performSchemaAction();

		verify(factoryBean, never()).createTables(anyBoolean(), anyBoolean(), anyBoolean());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void createsTableForEntity() throws Exception {
		Metadata mockMetadata = mock(Metadata.class);
		KeyspaceMetadata mockKeyspaceMetadata = mock(KeyspaceMetadata.class);
		CassandraMappingContext mockMappingContext = mock(CassandraMappingContext.class);
		CassandraPersistentEntity<Person> mockPersistentEntity = mock(CassandraPersistentEntity.class);
		CassandraAdminOperations mockCassandraAdminOperations = mock(CassandraAdminOperations.class);

		doReturn(mockCassandraAdminOperations).when(factoryBean).getCassandraAdminOperations();
		doReturn(mockSession).when(factoryBean).getObject();
		when(mockCluster.getMetadata()).thenReturn(mockMetadata);
		when(mockMetadata.getKeyspace(eq("TestKeyspace"))).thenReturn(mockKeyspaceMetadata);
		when(mockKeyspaceMetadata.getTables()).thenReturn(Collections.<TableMetadata>emptyList());
		when(mockConverter.getMappingContext()).thenReturn(mockMappingContext);
		when(mockMappingContext.getNonPrimaryKeyEntities()).thenReturn(
			Collections.<CassandraPersistentEntity<?>>singletonList(mockPersistentEntity));
		when(mockPersistentEntity.getTableName()).thenReturn(newCqlIdentifier("TestTable"));
		when(mockPersistentEntity.getType()).thenReturn(Person.class);

		factoryBean.setConverter(mockConverter);
		factoryBean.setKeyspaceName("TestKeyspace");

		assertThat(factoryBean.getConverter(), is(equalTo(mockConverter)));

		factoryBean.createTables(true, false, false);

		verify(mockSession, times(1)).getCluster();
		verify(mockCluster, times(1)).getMetadata();
		verify(mockMetadata, times(1)).getKeyspace(eq("TestKeyspace"));
		verify(mockKeyspaceMetadata, times(1)).getTables();
		verify(mockConverter, times(1)).getMappingContext();
		verify(mockMappingContext, times(1)).getNonPrimaryKeyEntities();
		verify(mockPersistentEntity, times(1)).getTableName();
		verify(mockPersistentEntity, times(1)).getType();
		verify(mockCassandraAdminOperations, times(1)).createTable(eq(false), eq(newCqlIdentifier("TestTable")),
			eq(Person.class), isNull(Map.class));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void createTableForEntityIfNotExists() {
		CassandraMappingContext mockMappingContext = mock(CassandraMappingContext.class);
		CassandraPersistentEntity<Person> mockPersistentEntity = mock(CassandraPersistentEntity.class);
		CassandraAdminOperations mockCassandraAdminOperations = mock(CassandraAdminOperations.class);

		doReturn(mockCassandraAdminOperations).when(factoryBean).getCassandraAdminOperations();
		doReturn(mockSession).when(factoryBean).getObject();
		when(mockConverter.getMappingContext()).thenReturn(mockMappingContext);
		when(mockMappingContext.getNonPrimaryKeyEntities()).thenReturn(
			Collections.<CassandraPersistentEntity<?>>singletonList(mockPersistentEntity));
		when(mockPersistentEntity.getTableName()).thenReturn(newCqlIdentifier("TestTable"));
		when(mockPersistentEntity.getType()).thenReturn(Person.class);

		factoryBean.setConverter(mockConverter);
		factoryBean.setKeyspaceName("TestKeyspace");

		assertThat(factoryBean.getConverter(), is(equalTo(mockConverter)));

		factoryBean.createTables(false, false, true);

		verify(mockSession, never()).getCluster();
		verify(mockCluster, never()).getMetadata();
		verify(mockConverter, times(1)).getMappingContext();
		verify(mockMappingContext, times(1)).getNonPrimaryKeyEntities();
		verify(mockPersistentEntity, times(1)).getTableName();
		verify(mockPersistentEntity, times(1)).getType();
		verify(mockCassandraAdminOperations, times(1)).createTable(eq(true), eq(newCqlIdentifier("TestTable")),
			eq(Person.class), isNull(Map.class));
	}

	@Test
	public void createTableThrowsIllegalStateExceptionWhenKeyspaceNotFound() {
		Metadata mockMetadata = mock(Metadata.class);

		doReturn(mockSession).when(factoryBean).getObject();
		when(mockCluster.getMetadata()).thenReturn(mockMetadata);
		when(mockMetadata.getKeyspace(anyString())).thenReturn(null);

		exception.expect(IllegalStateException.class);
		exception.expectCause(is(nullValue(Throwable.class)));
		exception.expectMessage("keyspace [TestKeyspace] does not exist");

		factoryBean.setKeyspaceName("TestKeyspace");
		factoryBean.createTables(true, false, true);

		verify(mockSession, times(1)).getCluster();
		verify(mockCluster, times(1)).getMetadata();
		verify(mockMetadata, times(1)).getKeyspace(eq("TestKeyspace"));
		verify(mockMetadata, times(1)).getKeyspace(eq("testkeyspace"));
	}

	// TODO: add more createTable tests covering drop tables, etc

	@Test
	public void setAndGetConverter() {
		assertThat(factoryBean.getConverter(), is(nullValue()));
		factoryBean.setConverter(mockConverter);
		assertThat(factoryBean.getConverter(), is(equalTo(mockConverter)));
		verifyZeroInteractions(mockConverter);
	}

	@Test
	public void setConverterToNull() {
		exception.expect(IllegalArgumentException.class);
		exception.expectCause(is(nullValue(Throwable.class)));
		exception.expectMessage("CassandraConverter must not be null");

		factoryBean.setConverter(null);
	}

	@Test
	public void setAndGetSchemaAction() {
		assertThat(factoryBean.getSchemaAction(), is(equalTo(SchemaAction.NONE)));
		factoryBean.setSchemaAction(SchemaAction.CREATE);
		assertThat(factoryBean.getSchemaAction(), is(equalTo(SchemaAction.CREATE)));
		factoryBean.setSchemaAction(SchemaAction.NONE);
		assertThat(factoryBean.getSchemaAction(), is(equalTo(SchemaAction.NONE)));
	}

	@Test
	public void setSchemaActionToNullThrowsIllegalArgumentException() {
		exception.expect(IllegalArgumentException.class);
		exception.expectCause(is(nullValue(Throwable.class)));
		exception.expectMessage("SchemaAction must not be null");

		factoryBean.setSchemaAction(null);
	}

	static class Person {
	}

}

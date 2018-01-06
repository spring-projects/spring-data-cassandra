/*
 * Copyright 2013-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.config;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.springframework.data.cassandra.config.CassandraSessionFactoryBean.*;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.cassandra.core.convert.CassandraConverter;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * The CassandraSessionFactoryBeanUnitTests class is a test suite of test cases testing the contract and functionality
 * of the {@link CassandraSessionFactoryBean} class.
 *
 * @author John Blum
 * @see org.springframework.data.cassandra.config.CassandraSessionFactoryBean
 */
@RunWith(MockitoJUnitRunner.class)
public class CassandraSessionFactoryBeanUnitTests {

	@Rule public ExpectedException exception = ExpectedException.none();

	@Mock CassandraConverter mockConverter;
	@Mock Cluster mockCluster;
	@Mock Session mockSession;

	CassandraSessionFactoryBean factoryBean;

	@Before
	public void setup() {

		when(mockCluster.connect()).thenReturn(mockSession);

		factoryBean = spy(new CassandraSessionFactoryBean());
		factoryBean.setCluster(mockCluster);
	}

	@Test // DATACASS-219
	public void afterPropertiesSetPerformsSchemaAction() throws Exception {

		doAnswer(invocationOnMock -> {
			assertThat(factoryBean.getSchemaAction()).isEqualTo(SchemaAction.RECREATE);
			return null;
		}).when(factoryBean).performSchemaAction();

		factoryBean.setConverter(mockConverter);
		factoryBean.setSchemaAction(SchemaAction.RECREATE);

		assertThat(factoryBean.getConverter()).isEqualTo(mockConverter);
		assertThat(factoryBean.getSchemaAction()).isEqualTo(SchemaAction.RECREATE);

		factoryBean.afterPropertiesSet();

		assertThat(factoryBean.getCassandraAdminOperations()).isNotNull();
		assertThat(factoryBean.getObject()).isEqualTo(mockSession);

		verify(factoryBean, times(1)).performSchemaAction();
	}

	@Test // DATACASS-219
	public void afterPropertiesSetThrowsIllegalStateExceptionWhenConverterIsNull() throws Exception {

		exception.expect(IllegalStateException.class);
		exception.expectMessage("Converter was not properly initialized");

		factoryBean.setCluster(mockCluster);
		factoryBean.afterPropertiesSet();
	}

	private void performSchemaActionCallsCreateTableWithArgumentsMatchingTheSchemaAction(SchemaAction schemaAction,
			boolean dropTables, boolean dropUnused, boolean ifNotExists) {

		doAnswer(invocationOnMock -> {
			assertThat(invocationOnMock.<Boolean> getArgument(0)).isEqualTo(dropTables);
			assertThat(invocationOnMock.<Boolean> getArgument(1)).isEqualTo(dropUnused);
			assertThat(invocationOnMock.<Boolean> getArgument(2)).isEqualTo(ifNotExists);
			return null;
		}).when(factoryBean).createTables(anyBoolean(), anyBoolean(), anyBoolean());

		factoryBean.setSchemaAction(schemaAction);
		assertThat(factoryBean.getSchemaAction()).isEqualTo(schemaAction);

		factoryBean.performSchemaAction();
		verify(factoryBean, times(1)).createTables(eq(dropTables), eq(dropUnused), eq(ifNotExists));
	}

	@Test // DATACASS-219
	public void performsSchemaActionCreatesTablesWithDefaults() {
		performSchemaActionCallsCreateTableWithArgumentsMatchingTheSchemaAction(SchemaAction.CREATE, DEFAULT_DROP_TABLES,
				DEFAULT_DROP_UNUSED_TABLES, DEFAULT_CREATE_IF_NOT_EXISTS);
	}

	@Test // DATACASS-219
	public void performsSchemaActionCreatesTablesIfNotExists() {
		performSchemaActionCallsCreateTableWithArgumentsMatchingTheSchemaAction(SchemaAction.CREATE_IF_NOT_EXISTS,
				DEFAULT_DROP_TABLES, DEFAULT_DROP_UNUSED_TABLES, true);
	}

	@Test // DATACASS-219
	public void performsSchemaActionRecreatesTables() {
		performSchemaActionCallsCreateTableWithArgumentsMatchingTheSchemaAction(SchemaAction.RECREATE, true,
				DEFAULT_DROP_UNUSED_TABLES, DEFAULT_CREATE_IF_NOT_EXISTS);
	}

	@Test // DATACASS-219
	public void performsSchemaActionRecreatesAndDropsUnusedTables() {
		performSchemaActionCallsCreateTableWithArgumentsMatchingTheSchemaAction(SchemaAction.RECREATE_DROP_UNUSED, true,
				true, DEFAULT_CREATE_IF_NOT_EXISTS);
	}

	@Test // DATACASS-219
	public void performsSchemaActionDoesNotCallCreateTablesWhenSchemaActionIsNone() {

		factoryBean.setSchemaAction(SchemaAction.NONE);

		assertThat(factoryBean.getSchemaAction()).isEqualTo(SchemaAction.NONE);

		factoryBean.performSchemaAction();

		verify(factoryBean, never()).createTables(anyBoolean(), anyBoolean(), anyBoolean());
	}

	@Test // DATACASS-219
	public void setAndGetConverter() {

		assertThat(factoryBean.getConverter()).isNull();
		factoryBean.setConverter(mockConverter);
		assertThat(factoryBean.getConverter()).isEqualTo(mockConverter);
		verifyZeroInteractions(mockConverter);
	}

	@Test // DATACASS-219
	public void setConverterToNull() {

		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("CassandraConverter must not be null");

		factoryBean.setConverter(null);
	}

	@Test // DATACASS-219
	public void setAndGetSchemaAction() {

		assertThat(factoryBean.getSchemaAction()).isEqualTo(SchemaAction.NONE);
		factoryBean.setSchemaAction(SchemaAction.CREATE);
		assertThat(factoryBean.getSchemaAction()).isEqualTo(SchemaAction.CREATE);
		factoryBean.setSchemaAction(SchemaAction.NONE);
		assertThat(factoryBean.getSchemaAction()).isEqualTo(SchemaAction.NONE);
	}

	@Test // DATACASS-219
	public void setSchemaActionToNullThrowsIllegalArgumentException() {

		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("SchemaAction must not be null");

		factoryBean.setSchemaAction(null);
	}

	static class Person {}
}

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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;

import com.datastax.driver.core.Session;

/**
 * Unit tests for {@link CassandraMappingBeanFactoryPostProcessor}.
 *
 * @author Mark Paluch
 * @author Mateusz Szymczak
 */
public class CassandraMappingBeanFactoryPostProcessorUnitTests {

	@Rule public final ExpectedException expectedException = ExpectedException.none();

	@Test // DATACASS-290, DATACASS-401
	public void clusterRegistrationTriggersDefaultBeanRegistration() {

		GenericXmlApplicationContext context = new GenericXmlApplicationContext();
		context.load(CassandraMappingBeanFactoryPostProcessorUnitTests.class, "cluster-and-mock-session.xml");
		context.refresh();

		assertThat(context.getBeanNamesForType(CassandraOperations.class), hasItemInArray(DefaultBeanNames.DATA_TEMPLATE));
		assertThat(context.getBeanNamesForType(CassandraMappingContext.class), hasItemInArray(DefaultBeanNames.CONTEXT));
		assertThat(context.getBeanNamesForType(CassandraConverter.class), hasItemInArray(DefaultBeanNames.CONVERTER));
	}

	@Test // DATACASS-290, DATACASS-401
	public void MappingAndConverterRegistrationTriggersDefaultBeanRegistration() {

		GenericXmlApplicationContext context = new GenericXmlApplicationContext();
		context.load(CassandraMappingBeanFactoryPostProcessorUnitTests.class, "mock-session-mapping-converter.xml");
		context.refresh();

		assertThat(context.getBeanNamesForType(CassandraOperations.class), hasItemInArray(DefaultBeanNames.DATA_TEMPLATE));
	}

	@Test // DATACASS-290
	public void converterRegistrationFailsDueToMissingCassandraMapping() {

		expectedException.expect(BeanCreationException.class);
		expectedException.expectMessage(containsString("No bean named 'cassandraMapping'"));

		GenericXmlApplicationContext context = new GenericXmlApplicationContext();
		context.load(CassandraMappingBeanFactoryPostProcessorUnitTests.class, "mock-session-converter.xml");
		context.refresh();
	}

	@Test // DATACASS-290
	public void defaultBeanRegistrationShouldFailWithMultipleSessions() {

		expectedException.expect(IllegalStateException.class);
		expectedException.expectMessage(allOf(containsString("found 2 beans of type"), containsString("Session")));

		GenericXmlApplicationContext context = new GenericXmlApplicationContext();
		context.load(CassandraMappingBeanFactoryPostProcessorUnitTests.class, "multiple-sessions.xml");
		context.refresh();
	}

	@Test // DATACASS-290
	public void defaultBeanRegistrationShouldFailWithMultipleSessionFactories() {

		expectedException.expect(IllegalStateException.class);
		expectedException.expectMessage(allOf(containsString("found 2 beans of type"), containsString("Session")));

		GenericXmlApplicationContext context = new GenericXmlApplicationContext();
		context.load(CassandraMappingBeanFactoryPostProcessorUnitTests.class, "multiple-session-factories.xml");
		context.refresh();
	}

	@Test // DATACASS-290
	public void defaultBeanRegistrationShouldFailWithMultipleMappingContexts() {

		expectedException.expect(IllegalStateException.class);
		expectedException
				.expectMessage(allOf(containsString("found 2 beans of type"), containsString("CassandraMappingContext")));

		GenericXmlApplicationContext context = new GenericXmlApplicationContext();
		context.load(CassandraMappingBeanFactoryPostProcessorUnitTests.class, "multiple-mapping-contexts.xml");
		context.refresh();
	}

	@Test // DATACASS-290
	public void defaultBeanRegistrationShouldFailWithMultipleConvertersContexts() {

		expectedException.expect(IllegalStateException.class);
		expectedException
				.expectMessage(allOf(containsString("found 2 beans of type"), containsString("CassandraConverter")));

		GenericXmlApplicationContext context = new GenericXmlApplicationContext();
		context.load(CassandraMappingBeanFactoryPostProcessorUnitTests.class, "multiple-converters.xml");
		context.refresh();
	}

	@Test // DATACASS-290
	public void shouldAllowTwoKeyspaces() {

		GenericXmlApplicationContext context = new GenericXmlApplicationContext();
		context.load(CassandraMappingBeanFactoryPostProcessorUnitTests.class, "two-keyspaces-namespace.xml");
		context.refresh();

		assertThat(context.getBeanNamesForType(CassandraOperations.class), arrayContaining("c-1", "c-2"));
		assertThat(context.getBeanNamesForType(CassandraMappingContext.class), arrayContaining("mapping-1", "mapping-2"));
		assertThat(context.getBeanNamesForType(CassandraConverter.class), arrayContaining("converter-1", "converter-2"));
	}

	@SuppressWarnings("unused")
	private static class MockSessionFactory extends AbstractFactoryBean<Session> {

		@Override
		public Class<?> getObjectType() {
			return Session.class;
		}

		@Override
		protected Session createInstance() {
			return mock(Session.class);
		}
	}
}

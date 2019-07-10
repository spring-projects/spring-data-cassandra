/*
 * Copyright 2016-2019 the original author or authors.
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
import static org.mockito.Mockito.*;

import org.junit.Test;

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

	@Test // DATACASS-290, DATACASS-401
	public void clusterRegistrationTriggersDefaultBeanRegistration() {

		GenericXmlApplicationContext context = new GenericXmlApplicationContext();
		context.load(CassandraMappingBeanFactoryPostProcessorUnitTests.class, "cluster-and-mock-session.xml");
		context.refresh();

		assertThat(context.getBeanNamesForType(CassandraOperations.class)).contains(DefaultBeanNames.DATA_TEMPLATE);
		assertThat(context.getBeanNamesForType(CassandraMappingContext.class)).contains(DefaultBeanNames.CONTEXT);
		assertThat(context.getBeanNamesForType(CassandraConverter.class)).contains(DefaultBeanNames.CONVERTER);
	}

	@Test // DATACASS-290, DATACASS-401
	public void MappingAndConverterRegistrationTriggersDefaultBeanRegistration() {

		GenericXmlApplicationContext context = new GenericXmlApplicationContext();
		context.load(CassandraMappingBeanFactoryPostProcessorUnitTests.class, "mock-session-mapping-converter.xml");
		context.refresh();

		assertThat(context.getBeanNamesForType(CassandraOperations.class)).contains(DefaultBeanNames.DATA_TEMPLATE);
	}

	@Test // DATACASS-290
	public void converterRegistrationFailsDueToMissingCassandraMapping() {

		GenericXmlApplicationContext context = new GenericXmlApplicationContext();
		context.load(CassandraMappingBeanFactoryPostProcessorUnitTests.class, "mock-session-converter.xml");

		assertThatExceptionOfType(BeanCreationException.class).isThrownBy(context::refresh)
				.withMessageContaining("No bean named 'cassandraMapping'");
	}

	@Test // DATACASS-290
	public void defaultBeanRegistrationShouldFailWithMultipleSessions() {

		GenericXmlApplicationContext context = new GenericXmlApplicationContext();
		context.load(CassandraMappingBeanFactoryPostProcessorUnitTests.class, "multiple-sessions.xml");

		assertThatIllegalStateException().isThrownBy(context::refresh).withMessageContaining("found 2 beans of type")
				.withMessageContaining("Session");
	}

	@Test // DATACASS-290
	public void defaultBeanRegistrationShouldFailWithMultipleSessionFactories() {

		GenericXmlApplicationContext context = new GenericXmlApplicationContext();
		context.load(CassandraMappingBeanFactoryPostProcessorUnitTests.class, "multiple-session-factories.xml");

		assertThatIllegalStateException().isThrownBy(context::refresh).withMessageContaining("found 2 beans of type")
				.withMessageContaining("Session");
	}

	@Test // DATACASS-290
	public void defaultBeanRegistrationShouldFailWithMultipleMappingContexts() {

		GenericXmlApplicationContext context = new GenericXmlApplicationContext();
		context.load(CassandraMappingBeanFactoryPostProcessorUnitTests.class, "multiple-mapping-contexts.xml");

		assertThatIllegalStateException().isThrownBy(context::refresh).withMessageContaining("found 2 beans of type")
				.withMessageContaining("CassandraMappingContext");
	}

	@Test // DATACASS-290
	public void defaultBeanRegistrationShouldFailWithMultipleConvertersContexts() {

		GenericXmlApplicationContext context = new GenericXmlApplicationContext();
		context.load(CassandraMappingBeanFactoryPostProcessorUnitTests.class, "multiple-converters.xml");

		assertThatIllegalStateException().isThrownBy(context::refresh).withMessageContaining("found 2 beans of type")
				.withMessageContaining("CassandraConverter");
	}

	@Test // DATACASS-290
	public void shouldAllowTwoKeyspaces() {

		GenericXmlApplicationContext context = new GenericXmlApplicationContext();
		context.load(CassandraMappingBeanFactoryPostProcessorUnitTests.class, "two-keyspaces-namespace.xml");
		context.refresh();

		assertThat(context.getBeanNamesForType(CassandraOperations.class)).containsExactly("c-1", "c-2");
		assertThat(context.getBeanNamesForType(CassandraMappingContext.class)).containsExactly("mapping-1", "mapping-2");
		assertThat(context.getBeanNamesForType(CassandraConverter.class)).containsExactly("converter-1", "converter-2");
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

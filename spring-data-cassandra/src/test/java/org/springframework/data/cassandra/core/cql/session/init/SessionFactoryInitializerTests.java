/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.session.init;

import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.cassandra.SessionFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;

/**
 * Unit tests for {@link org.springframework.data.cassandra.core.cql.session.init.SessionFactoryInitializer}.
 *
 * @author Mark Paluch
 */
public class SessionFactoryInitializerTests {

	@Test // DATACASS-704
	void shouldInitializeKeyspace() {

		CqlSession session = initialize("initialize-keyspace.xml");

		verify(session).execute("create table if not exists mytable1 (id uuid primary key, column1 text)");
		verify(session).execute("create table if not exists mytable2 (id uuid primary key, column1 text)");
		verifyNoMoreInteractions(session);
	}

	@Test // DATACASS-704
	void shouldInitializeAndCleanupKeyspace() {

		CqlSession session = initialize("initialize-and-cleanup-keyspace.xml");

		verify(session).execute("create table if not exists mytable1 (id uuid primary key, column1 text)");
		verify(session).execute("create table if not exists mytable2 (id uuid primary key, column1 text)");
		verify(session).execute("drop table mytable1");
		verify(session).execute("drop table mytable2");
		verifyNoMoreInteractions(session);
	}

	private ClassPathXmlApplicationContext context(String file) {
		return new ClassPathXmlApplicationContext(file, getClass());
	}

	private CqlSession initialize(String file) {
		ConfigurableApplicationContext context = context(file);
		try {
			return context.getBean(SessionFactory.class).getSession();
		} finally {
			context.close();
		}
	}

	@SuppressWarnings("unused")
	private static class MockSessionFactoryFactoryBean extends AbstractFactoryBean<SessionFactory> {

		@Override
		public Class<?> getObjectType() {
			return SessionFactory.class;
		}

		@Override
		protected SessionFactory createInstance() {

			CqlSession sessionMock = mock(CqlSession.class);
			SessionFactory sessionFactoryMock = mock(SessionFactory.class);

			when(sessionFactoryMock.getSession()).thenReturn(sessionMock);
			when(sessionMock.execute(anyString())).thenReturn(mock(ResultSet.class));

			return sessionFactoryMock;
		}
	}
}

/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.support;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Unit tests for {@link CachedPreparedStatementCreator}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class CachedPreparedStatementCreatorUnitTests {

	@Mock Session session;

	@Mock Session otherSession;

	@Mock Session otherKeyspaceSession;

	@Mock Cluster cluster;

	@Mock PreparedStatement preparedStatement;

	@Before
	public void before() {

		when(session.getCluster()).thenReturn(cluster);
		when(otherSession.getCluster()).thenReturn(cluster);
		when(otherKeyspaceSession.getCluster()).thenReturn(cluster);

		when(session.getLoggedKeyspace()).thenReturn("keyspace");
		when(otherSession.getLoggedKeyspace()).thenReturn("keyspace");
		when(otherKeyspaceSession.getLoggedKeyspace()).thenReturn("other");

		when(session.prepare(any(RegularStatement.class))).thenReturn(preparedStatement);
	}

	@Test // DATACASS-403
	public void shouldPrepareStatement() {

		String cql = "SELECT foo FROM users;";

		MapPreparedStatementCache cache = MapPreparedStatementCache.create();

		CachedPreparedStatementCreator creator = CachedPreparedStatementCreator.of(cache, cql);

		PreparedStatement result = creator.createPreparedStatement(session);

		assertThat(result).isSameAs(preparedStatement);
		assertThat(creator.getCache()).isSameAs(cache);
		assertThat(cache.getCache()).hasSize(1);
	}

	@Test // DATACASS-403
	public void shouldCachePreparedStatement() {

		String cql = "SELECT foo FROM users;";

		PreparedStatementCache cache = PreparedStatementCache.create();

		assertThat(CachedPreparedStatementCreator.of(cache, cql).createPreparedStatement(session))
				.isSameAs(preparedStatement);
		assertThat(CachedPreparedStatementCreator.of(cache, cql).createPreparedStatement(session))
				.isSameAs(preparedStatement);

		verify(session, atMost(1)).prepare(any(SimpleStatement.class));
	}

	@Test // DATACASS-403
	public void shouldCachePreparedStatementAcrossSessions() {

		String cql = "SELECT foo FROM users;";

		PreparedStatementCache cache = PreparedStatementCache.create();

		CachedPreparedStatementCreator creator = CachedPreparedStatementCreator.of(cache, cql);

		assertThat(creator.createPreparedStatement(session)).isSameAs(preparedStatement);
		assertThat(creator.createPreparedStatement(otherSession)).isSameAs(preparedStatement);

		verify(session, atMost(1)).prepare(any(SimpleStatement.class));
		verify(otherSession, never()).prepare(any(SimpleStatement.class));
	}

	@Test // DATACASS-403
	public void shouldCachePreparedStatementOnKeyspaceLevel() {

		String cql = "SELECT foo FROM users;";
		when(otherKeyspaceSession.prepare(any(RegularStatement.class))).thenReturn(preparedStatement);

		PreparedStatementCache cache = PreparedStatementCache.create();

		CachedPreparedStatementCreator creator = CachedPreparedStatementCreator.of(cache, cql);

		assertThat(creator.createPreparedStatement(session)).isSameAs(preparedStatement);
		assertThat(creator.createPreparedStatement(otherKeyspaceSession)).isSameAs(preparedStatement);

		verify(session).prepare(any(SimpleStatement.class));
		verify(otherKeyspaceSession).prepare(any(SimpleStatement.class));
	}

	@Test // DATACASS-403
	public void shouldCacheBuiltPreparedStatement() {

		RegularStatement statement = QueryBuilder.update("users").with(QueryBuilder.set("foo", "bar"));

		PreparedStatementCache cache = PreparedStatementCache.create();

		when(session.prepare(statement)).thenReturn(preparedStatement);

		assertThat(CachedPreparedStatementCreator.of(cache, statement).createPreparedStatement(session))
				.isSameAs(preparedStatement);
		assertThat(CachedPreparedStatementCreator.of(cache, statement).createPreparedStatement(session))
				.isSameAs(preparedStatement);

		assertThat(statement.isIdempotent()).isTrue();
		verify(session).prepare(statement);
	}

	@Test // DATACASS-403
	public void shouldCacheSameBuiltPreparedStatements() {

		RegularStatement firstStatement = QueryBuilder.update("users").with(QueryBuilder.set("foo", "bar"));
		RegularStatement secondStatement = QueryBuilder.update("users").with(QueryBuilder.set("foo", "bar"));

		PreparedStatementCache cache = PreparedStatementCache.create();

		when(session.prepare(firstStatement)).thenReturn(preparedStatement);

		CachedPreparedStatementCreator.of(cache, firstStatement).createPreparedStatement(session);
		CachedPreparedStatementCreator.of(cache, secondStatement).createPreparedStatement(session);

		verify(session).prepare(firstStatement);
	}

	@Test // DATACASS-403
	public void shouldCacheAdoptDifferencesInCachedPreparedStatements() {

		RegularStatement firstStatement = QueryBuilder.update("users").with(QueryBuilder.set("foo", "bar"));
		RegularStatement secondStatement = QueryBuilder.update("users").with(QueryBuilder.set("bar", "foo"));

		PreparedStatementCache cache = PreparedStatementCache.create();

		when(session.prepare(firstStatement)).thenReturn(preparedStatement);
		when(session.prepare(secondStatement)).thenReturn(preparedStatement);

		CachedPreparedStatementCreator.of(cache, firstStatement).createPreparedStatement(session);
		CachedPreparedStatementCreator.of(cache, secondStatement).createPreparedStatement(session);

		verify(session).prepare(firstStatement);
		verify(session).prepare(secondStatement);
	}
}

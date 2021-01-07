/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.support;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;

/**
 * Unit tests for {@link CachedPreparedStatementCreator}.
 *
 * @author Mark Paluch
 * @author Aldo Bongio
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CachedPreparedStatementCreatorUnitTests {

	@Mock CqlSession session;

	@Mock CqlSession otherSession;

	@Mock CqlSession otherKeyspaceSession;

	@Mock PreparedStatement preparedStatement;

	@BeforeEach
	void before() {

		when(session.getKeyspace()).thenReturn(Optional.of(CqlIdentifier.fromCql("mykeyspace")));
		when(otherSession.getKeyspace()).thenReturn(Optional.of(CqlIdentifier.fromCql("mykeyspace")));
		when(otherKeyspaceSession.getKeyspace()).thenReturn(Optional.of(CqlIdentifier.fromCql("other")));

		when(session.prepare(any(SimpleStatement.class))).thenReturn(preparedStatement);
	}

	@Test // DATACASS-403
	void shouldPrepareStatement() {

		String cql = "SELECT foo FROM users;";

		MapPreparedStatementCache cache = MapPreparedStatementCache.create();

		CachedPreparedStatementCreator creator = CachedPreparedStatementCreator.of(cache, cql);

		PreparedStatement result = creator.createPreparedStatement(session);

		assertThat(result).isSameAs(preparedStatement);
		assertThat(creator.getCache()).isSameAs(cache);
		assertThat(cache.getCache()).hasSize(1);
	}

	@Test // DATACASS-403
	void shouldCachePreparedStatement() {

		String cql = "SELECT foo FROM users;";

		PreparedStatementCache cache = PreparedStatementCache.create();

		assertThat(CachedPreparedStatementCreator.of(cache, cql).createPreparedStatement(session))
				.isSameAs(preparedStatement);
		assertThat(CachedPreparedStatementCreator.of(cache, cql).createPreparedStatement(session))
				.isSameAs(preparedStatement);

		verify(session, atMost(1)).prepare(any(SimpleStatement.class));
	}

	@Test // DATACASS-403
	void shouldCachePreparedStatementAcrossSessions() {

		String cql = "SELECT foo FROM users;";

		PreparedStatementCache cache = PreparedStatementCache.create();

		CachedPreparedStatementCreator creator = CachedPreparedStatementCreator.of(cache, cql);

		assertThat(creator.createPreparedStatement(session)).isSameAs(preparedStatement);
		assertThat(creator.createPreparedStatement(otherSession)).isSameAs(preparedStatement);

		verify(session, atMost(1)).prepare(any(SimpleStatement.class));
		verify(otherSession, never()).prepare(any(SimpleStatement.class));
	}

	@Test // DATACASS-403
	void shouldCachePreparedStatementOnKeyspaceLevel() {

		String cql = "SELECT foo FROM users;";
		when(otherKeyspaceSession.prepare(any(SimpleStatement.class))).thenReturn(preparedStatement);

		PreparedStatementCache cache = PreparedStatementCache.create();

		CachedPreparedStatementCreator creator = CachedPreparedStatementCreator.of(cache, cql);

		assertThat(creator.createPreparedStatement(session)).isSameAs(preparedStatement);
		assertThat(creator.createPreparedStatement(otherKeyspaceSession)).isSameAs(preparedStatement);

		verify(session).prepare(any(SimpleStatement.class));
		verify(otherKeyspaceSession).prepare(any(SimpleStatement.class));
	}

	@Test // DATACASS-403
	void shouldCacheBuiltPreparedStatement() {

		SimpleStatement statement = QueryBuilder.update("users")
				.set(Assignment.setColumn("foo", QueryBuilder.literal("bar"))).where().build();

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
	void shouldCacheSameBuiltPreparedStatements() {

		SimpleStatement firstStatement = QueryBuilder.update("users")
				.set(Assignment.setColumn("foo", QueryBuilder.literal("bar"))).where().build();
		SimpleStatement secondStatement = QueryBuilder.update("users")
				.set(Assignment.setColumn("foo", QueryBuilder.literal("bar"))).where().build();

		PreparedStatementCache cache = PreparedStatementCache.create();

		when(session.prepare(firstStatement)).thenReturn(preparedStatement);

		CachedPreparedStatementCreator.of(cache, firstStatement).createPreparedStatement(session);
		CachedPreparedStatementCreator.of(cache, secondStatement).createPreparedStatement(session);

		verify(session).prepare(firstStatement);
	}

	@Test // DATACASS-403
	void shouldCacheAdoptDifferencesInCachedPreparedStatements() {

		SimpleStatement firstStatement = QueryBuilder.update("users")
				.set(Assignment.setColumn("foo", QueryBuilder.literal("bar"))).where().build();
		SimpleStatement secondStatement = QueryBuilder.update("users")
				.set(Assignment.setColumn("boo", QueryBuilder.literal("far"))).where().build();

		PreparedStatementCache cache = PreparedStatementCache.create();

		when(session.prepare(firstStatement)).thenReturn(preparedStatement);
		when(session.prepare(secondStatement)).thenReturn(preparedStatement);

		CachedPreparedStatementCreator.of(cache, firstStatement).createPreparedStatement(session);
		CachedPreparedStatementCreator.of(cache, secondStatement).createPreparedStatement(session);

		verify(session).prepare(firstStatement);
		verify(session).prepare(secondStatement);
	}

	@Test // DATACASS-814
	void shouldUseCqlTextInCacheKey() {

		String cql = "SELECT foo FROM users;";

		MapPreparedStatementCache cache = MapPreparedStatementCache.create();
		CachedPreparedStatementCreator creator = CachedPreparedStatementCreator.of(cache, cql);
		creator.createPreparedStatement(session);

		MapPreparedStatementCache.CacheKey cacheKey = cache.getCache().keySet().iterator().next();
		assertThat(cacheKey.cql).isEqualTo(cql);
	}
}

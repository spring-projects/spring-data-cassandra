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

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

/**
 * Integration tests for {@link CachedPreparedStatementCreator}.
 *
 * @author Mark Paluch
 */
class CachedPreparedStatementCreatorIntegrationTest extends AbstractKeyspaceCreatingIntegrationTests {

	private static final AtomicBoolean initialized = new AtomicBoolean();

	@BeforeEach
	void before() throws Exception {

		if (initialized.compareAndSet(false, true)) {
			getSession().execute("CREATE TABLE IF NOT EXISTS user (id text PRIMARY KEY, username text);");
		} else {
			session.execute("TRUNCATE user;");
		}
	}

	@Test // DATACASS-403
	void shouldRetainIdempotencyFlag() {

		SimpleStatement insert = QueryBuilder.insertInto("user").value("id", QueryBuilder.bindMarker())
				.value("username", QueryBuilder.bindMarker()).build();

		assertThat(insert.isIdempotent()).isTrue();

		PreparedStatementCache cache = PreparedStatementCache.create();

		PreparedStatement preparedStatement = CachedPreparedStatementCreator.of(cache, insert)
				.createPreparedStatement(session);

		assertThat(preparedStatement.bind(1, 2).isIdempotent()).isTrue();
	}
}

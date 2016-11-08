/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.cassandra.core;

import static org.assertj.core.api.Assertions.*;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.SyntaxError;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Integration tests for {@link DefaultBridgedReactiveSession}.
 * 
 * @author Mark Paluch
 */
public class DefaultBridgedReactiveSessionIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	private DefaultBridgedReactiveSession reactiveSession;

	@Before
	public void before() throws Exception {

		this.session.execute("DROP TABLE IF EXISTS users;");

		this.reactiveSession = new DefaultBridgedReactiveSession(this.session, Schedulers.elastic());
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void executeShouldExecuteDeferred() throws Exception {

		Mono<ReactiveResultSet> execution = reactiveSession
				.execute("CREATE TABLE users (\n" + "  userid text PRIMARY KEY,\n" + "  first_name text\n" + ");");

		KeyspaceMetadata keyspace = getKeyspaceMetadata();

		assertThat(keyspace.getTable("users")).isNull();

		ReactiveResultSet resultSet = execution.block();
		assertThat(resultSet.wasApplied()).isTrue();
		assertThat(keyspace.getTable("users")).isNotNull();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void executeShouldTransportExceptionsInMono() throws Exception {

		Mono<ReactiveResultSet> execution = reactiveSession.execute("INSERT INTO dummy;");

		try {
			execution.block();
			fail("Missing SyntaxError");
		} catch (SyntaxError e) {
			assertThat(e).isInstanceOf(SyntaxError.class);
		}
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void executeShouldReturnRows() throws Exception {

		session.execute("CREATE TABLE users (\n" + "  userid text PRIMARY KEY,\n" + "  first_name text\n" + ");");
		session.execute("INSERT INTO users (userid, first_name) VALUES ('White', 'Walter');");

		Mono<ReactiveResultSet> execution = reactiveSession.execute("SELECT * FROM users;");
		ReactiveResultSet resultSet = execution.block();
		Row row = resultSet.rows().blockFirst();

		assertThat(row).isNotNull();
		assertThat(row.getString("userid")).isEqualTo("White");
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void executeShouldPrepareStatement() throws Exception {

		session.execute("CREATE TABLE users (\n" + "  userid text PRIMARY KEY,\n" + "  first_name text\n" + ");");

		Mono<PreparedStatement> execution = reactiveSession
				.prepare("INSERT INTO users (userid, first_name) VALUES (?, ?);");
		PreparedStatement preparedStatement = execution.block();

		assertThat(preparedStatement).isNotNull();
		assertThat(preparedStatement.getQueryString()).isEqualTo("INSERT INTO users (userid, first_name) VALUES (?, ?);");
	}

	private KeyspaceMetadata getKeyspaceMetadata() {
		return cluster.getMetadata().getKeyspace(this.session.getLoggedKeyspace());
	}
}

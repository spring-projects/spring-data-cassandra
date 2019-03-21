/*
 * Copyright 2016-2017 the original author or authors.
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
package org.springframework.cassandra.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.core.session.DefaultBridgedReactiveSession;
import org.springframework.cassandra.core.session.ReactiveResultSet;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.SyntaxError;

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

	@Test // DATACASS-335
	public void executeShouldExecuteDeferred() throws Exception {

		String query = "CREATE TABLE users (\n" + "  userid text PRIMARY KEY,\n" + "  first_name text\n" + ");";

		Mono<ReactiveResultSet> execution = reactiveSession.execute(query);

		KeyspaceMetadata keyspace = getKeyspaceMetadata();

		assertThat(keyspace.getTable("users")).isNull();

		ReactiveResultSet resultSet = execution.block();

		assertThat(resultSet.wasApplied()).isTrue();
		assertThat(keyspace.getTable("users")).isNotNull();
	}

	@Test(expected = SyntaxError.class) // DATACASS-335
	public void executeShouldTransportExceptionsInMono() throws Exception {

		Mono<ReactiveResultSet> execution = reactiveSession.execute("INSERT INTO dummy;");

		execution.block();
	}

	@Test // DATACASS-335
	public void executeShouldReturnRows() throws Exception {

		session.execute("CREATE TABLE users (\n" + "  userid text PRIMARY KEY,\n" + "  first_name text\n" + ");");
		session.execute("INSERT INTO users (userid, first_name) VALUES ('White', 'Walter');");

		Mono<ReactiveResultSet> execution = reactiveSession.execute("SELECT * FROM users;");
		ReactiveResultSet resultSet = execution.block();
		Row row = resultSet.rows().blockFirst();

		assertThat(row).isNotNull();
		assertThat(row.getString("userid")).isEqualTo("White");
	}

	@Test // DATACASS-335
	public void executeShouldPrepareStatement() throws Exception {

		session.execute("CREATE TABLE users (\n" + "  userid text PRIMARY KEY,\n" + "  first_name text\n" + ");");

		Mono<PreparedStatement> execution = reactiveSession.prepare(
				"INSERT INTO users (userid, first_name) VALUES (?, ?);");
		PreparedStatement preparedStatement = execution.block();

		assertThat(preparedStatement).isNotNull();
		assertThat(preparedStatement.getQueryString()).isEqualTo("INSERT INTO users (userid, first_name) VALUES (?, ?);");
	}

	private KeyspaceMetadata getKeyspaceMetadata() {
		return cluster.getMetadata().getKeyspace(this.session.getLoggedKeyspace());
	}
}

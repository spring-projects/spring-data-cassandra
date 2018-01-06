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
package org.springframework.data.cassandra.core.cql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import org.junit.Before;
import org.junit.Test;

import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.QueryLogger;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.SyntaxError;

/**
 * Integration tests for {@link DefaultBridgedReactiveSession}.
 *
 * @author Mark Paluch
 */
public class DefaultBridgedReactiveSessionIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	DefaultBridgedReactiveSession reactiveSession;

	@Before
	public void before() {

		this.session.execute("DROP TABLE IF EXISTS users;");

		this.reactiveSession = new DefaultBridgedReactiveSession(this.session, Schedulers.elastic());
	}

	@Test // DATACASS-335
	public void executeShouldExecuteDeferred() {

		String query = "CREATE TABLE users (\n" + "  userid text PRIMARY KEY,\n" + "  first_name text\n" + ");";

		Mono<ReactiveResultSet> execution = reactiveSession.execute(query);

		KeyspaceMetadata keyspace = getKeyspaceMetadata();

		assertThat(keyspace.getTable("users")).isNull();

		StepVerifier.create(execution)
			.consumeNextWith(actual -> assertThat(actual.wasApplied()).isTrue())
			.verifyComplete();

		assertThat(keyspace.getTable("users")).isNotNull();
	}

	@Test // DATACASS-335
	public void executeShouldTransportExceptionsInMono() {
		StepVerifier.create(reactiveSession.execute("INSERT INTO dummy;")).expectError(SyntaxError.class).verify();
	}

	@Test // DATACASS-335
	public void executeShouldReturnRows() {

		session.execute("CREATE TABLE users (\n" + "  userid text PRIMARY KEY,\n" + "  first_name text\n" + ");");
		session.execute("INSERT INTO users (userid, first_name) VALUES ('White', 'Walter');");

		StepVerifier.create(reactiveSession.execute("SELECT * FROM users;")).consumeNextWith(actual ->
			StepVerifier.create(actual.rows()).consumeNextWith(row ->
				assertThat(row.getString("userid")).isEqualTo("White")).verifyComplete()).verifyComplete();
	}

	@Test // DATACASS-335
	public void executeShouldPrepareStatement() {

		session.execute("CREATE TABLE users (\n" + "  userid text PRIMARY KEY,\n" + "  first_name text\n" + ");");

		StepVerifier.create(reactiveSession.prepare("INSERT INTO users (userid, first_name) VALUES (?, ?);"))
			.consumeNextWith(actual ->
				assertThat(actual.getQueryString()).isEqualTo("INSERT INTO users (userid, first_name) VALUES (?, ?);"))
			.verifyComplete();
	}

	@Test // DATACASS-509
	public void shouldFetchBatches() {

		String createTable = "CREATE TABLE users (\n" + "  userid text PRIMARY KEY,\n" + "  first_name text\n" + ");";

		this.session.execute(createTable);

		List<String> keys = new ArrayList<>();

		for (int i = 0; i < 100; i++) {

			String key = String.format("u-03%d", i);
			String value = "v-" + i;

			keys.add(key);

			this.session.execute(String.format("INSERT INTO users (userid, first_name) VALUES ('%s', '%s');", key, value));
		}

		this.session.getCluster().register(QueryLogger.builder().build());

		SimpleStatement statement = new SimpleStatement("SELECT * FROM users;");

		statement.setFetchSize(10);

		Mono<ReactiveResultSet> execution = reactiveSession.execute(statement);

		Collection<String> received = new ConcurrentLinkedQueue<>();

		StepVerifier.create(execution.flatMapMany(ReactiveResultSet::rows).map(row -> row.getString(0)))
				.recordWith(() -> received)
				.expectNextCount(100)
				.verifyComplete();

		assertThat(received).containsAll(keys).hasSize(100);
	}

	private KeyspaceMetadata getKeyspaceMetadata() {
		return cluster.getMetadata().getKeyspace(this.session.getLoggedKeyspace());
	}
}

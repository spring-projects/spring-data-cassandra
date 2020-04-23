/*
 * Copyright 2016-2020 the original author or authors.
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
package org.springframework.data.cassandra.core.cql;

import static org.assertj.core.api.Assertions.*;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import org.junit.Before;
import org.junit.Test;

import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;

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

		this.reactiveSession = new DefaultBridgedReactiveSession(this.session);
	}

	@Test // DATACASS-335
	public void executeShouldExecuteDeferred() {

		String query = "CREATE TABLE users (\n" + "  userid text PRIMARY KEY,\n" + "  first_name text\n" + ");";

		Mono<ReactiveResultSet> execution = reactiveSession.execute(query);

		assertThat(getKeyspaceMetadata().getTable("users")).isEmpty();

		execution.as(StepVerifier::create).consumeNextWith(actual -> assertThat(actual.wasApplied()).isTrue())
				.verifyComplete();

		assertThat(getKeyspaceMetadata().getTable("users")).isPresent();
	}

	@Test // DATACASS-335
	public void executeShouldTransportExceptionsInMono() {
		reactiveSession.execute("INSERT INTO dummy;").as(StepVerifier::create).expectError(SyntaxError.class).verify();
	}

	@Test // DATACASS-335
	public void executeShouldReturnRows() {

		session.execute("CREATE TABLE users (\n" + "  userid text PRIMARY KEY,\n" + "  first_name text\n" + ");");
		session.execute("INSERT INTO users (userid, first_name) VALUES ('White', 'Walter');");

		reactiveSession.execute("SELECT * FROM users;").as(StepVerifier::create)
				.consumeNextWith(actual -> actual.rows().as(StepVerifier::create)
						.consumeNextWith(row -> assertThat(row.getString("userid")).isEqualTo("White")).verifyComplete())
				.verifyComplete();
	}

	@Test // DATACASS-335
	public void executeShouldPrepareStatement() {

		session.execute("CREATE TABLE users (\n" + "  userid text PRIMARY KEY,\n" + "  first_name text\n" + ");");

		reactiveSession.prepare("INSERT INTO users (userid, first_name) VALUES (?, ?);").as(StepVerifier::create)
				.consumeNextWith(
						actual -> assertThat(actual.getQuery()).isEqualTo("INSERT INTO users (userid, first_name) VALUES (?, ?);"))
				.verifyComplete();
	}

	private KeyspaceMetadata getKeyspaceMetadata() {
		return this.session.getKeyspace().flatMap(it -> session.refreshSchema().getKeyspace(it)).get();
	}
}

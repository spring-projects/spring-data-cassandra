/*
 *  Copyright 2016-present the original author or authors.
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
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * Unit tests for {@link QueryOptionsUtil}.
 *
 * @author John Blum
 * @author Mark Paluch
 * @author Tomasz Lelek
 * @author Sam Lightfoot
 */
@ExtendWith(MockitoExtension.class)
class QueryOptionsUtilUnitTests {

	@Mock SimpleStatement simpleStatement;
	@Mock BoundStatement boundStatement;

	@Test // DATACASS-202
	void addStatementQueryOptionsShouldNotAddOptions() {

		QueryOptions queryOptions = QueryOptions.builder().build();

		QueryOptionsUtil.addQueryOptions(simpleStatement, queryOptions);

		verifyNoInteractions(simpleStatement);
	}

	@Test // DATACASS-202, GH-1220
	void addStatementQueryOptionsShouldAddGenericQueryOptions() {

		QueryOptions queryOptions = QueryOptions.builder() //
				.consistencyLevel(DefaultConsistencyLevel.QUORUM) //
				.executionProfile("foo") //
				.idempotent(true) //
				.pageSize(10) //
				.keyspace(CqlIdentifier.fromCql("ks1")) //
				.routingKeyspace(CqlIdentifier.fromCql("routing_ks")) //
				.routingKey(ByteBuffer.allocate(1)) //
				.serialConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL) //
				.build();

		SimpleStatement statement = QueryOptionsUtil.addQueryOptions(SimpleStatement.newInstance("SELECT * FROM person"),
				queryOptions);

		assertThat(statement.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.QUORUM);
		assertThat(statement.getExecutionProfileName()).isEqualTo("foo");
		assertThat(statement.isIdempotent()).isTrue();
		assertThat(statement.getPageSize()).isEqualTo(10);
		assertThat(statement.getKeyspace()).isEqualTo(CqlIdentifier.fromCql("ks1"));
		assertThat(statement.getRoutingKeyspace()).isEqualTo(CqlIdentifier.fromCql("routing_ks"));
		assertThat(statement.getRoutingKey()).isEqualTo(ByteBuffer.allocate(1));
		assertThat(statement.getSerialConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.LOCAL_SERIAL);
	}

	@Test // DATACASS-767
	void addKeyspaceOptionsOnBoundStatementShouldThrowException() {

		QueryOptions queryOptions = QueryOptions.builder() //
				.keyspace(CqlIdentifier.fromCql("ks1")).build();

		assertThatThrownBy(() -> QueryOptionsUtil.addQueryOptions(boundStatement, queryOptions))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("Keyspace cannot be set for a BoundStatement");
	}
}

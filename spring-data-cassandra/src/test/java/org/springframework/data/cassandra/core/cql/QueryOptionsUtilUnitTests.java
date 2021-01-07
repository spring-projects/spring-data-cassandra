/*
 *  Copyright 2016-2021 the original author or authors.
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

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * Unit tests for {@link QueryOptionsUtil}.
 *
 * @author John Blum
 * @author Mark Paluch
 * @author Tomasz Lelek
 */
@ExtendWith(MockitoExtension.class)
class QueryOptionsUtilUnitTests {

	@Mock SimpleStatement simpleStatement;
	@Mock BatchStatement batchStatement;
	@Mock BoundStatement boundStatement;

	@Test // DATACASS-202, DATACASS-708
	void addPreparedStatementOptionsShouldAddDriverQueryOptions() {

		when(simpleStatement.setConsistencyLevel(any())).thenReturn(simpleStatement);
		when(simpleStatement.setSerialConsistencyLevel(any())).thenReturn(simpleStatement);
		when(simpleStatement.setExecutionProfileName(anyString())).thenReturn(simpleStatement);

		QueryOptions queryOptions = QueryOptions.builder() //
				.consistencyLevel(DefaultConsistencyLevel.EACH_QUORUM) //
				.serialConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE) //
				.executionProfile("foo") //
				.build();

		QueryOptionsUtil.addQueryOptions(simpleStatement, queryOptions);

		verify(simpleStatement).setConsistencyLevel(DefaultConsistencyLevel.EACH_QUORUM);
		verify(simpleStatement).setSerialConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE);
		verify(simpleStatement).setExecutionProfileName("foo");
	}

	@Test // DATACASS-202
	void addStatementQueryOptionsShouldNotAddOptions() {

		QueryOptions queryOptions = QueryOptions.builder().build();

		QueryOptionsUtil.addQueryOptions(simpleStatement, queryOptions);

		verifyNoInteractions(simpleStatement);
	}

	@Test // DATACASS-202
	void addStatementQueryOptionsShouldAddGenericQueryOptions() {

		when(simpleStatement.setPageSize(anyInt())).thenReturn(simpleStatement);
		when(simpleStatement.setTimeout(any())).thenReturn(simpleStatement);
		when(simpleStatement.setTracing(anyBoolean())).thenReturn(simpleStatement);

		QueryOptions queryOptions = QueryOptions.builder() //
				.pageSize(10) //
				.readTimeout(1, TimeUnit.MINUTES) //
				.withTracing() //
				.build();

		QueryOptionsUtil.addQueryOptions(simpleStatement, queryOptions);

		verify(simpleStatement).setTimeout(Duration.ofMinutes(1));
		verify(simpleStatement).setPageSize(10);
		verify(simpleStatement).setTracing(true);
	}

	@Test // DATACASS-767
	void addKeyspaceOptionsOnSimpleStatementShouldAddDriverQueryOptions() {

		when(simpleStatement.setKeyspace(any(CqlIdentifier.class))).thenReturn(simpleStatement);

		QueryOptions queryOptions = QueryOptions.builder() //
				.keyspace(CqlIdentifier.fromCql("ks1")).build();

		QueryOptionsUtil.addQueryOptions(simpleStatement, queryOptions);

		verify(simpleStatement).setKeyspace(CqlIdentifier.fromCql("ks1"));
	}

	@Test // DATACASS-767
	void addKeyspaceOptionsOnBatchStatementShouldAddDriverQueryOptions() {

		when(batchStatement.setKeyspace(any(CqlIdentifier.class))).thenReturn(batchStatement);

		QueryOptions queryOptions = QueryOptions.builder() //
				.keyspace(CqlIdentifier.fromCql("ks1")).build();

		QueryOptionsUtil.addQueryOptions(batchStatement, queryOptions);

		verify(batchStatement).setKeyspace(CqlIdentifier.fromCql("ks1"));
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

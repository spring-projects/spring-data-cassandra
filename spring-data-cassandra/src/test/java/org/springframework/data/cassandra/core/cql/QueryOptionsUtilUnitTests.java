/*
 *  Copyright 2016-2018 the original author or authors.
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

import static org.mockito.Mockito.*;

import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Using;

/**
 * Unit tests for {@link QueryOptionsUtil}.
 *
 * @author John Blum
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class QueryOptionsUtilUnitTests {

	@Rule public ExpectedException exception = ExpectedException.none();

	@Mock Insert mockInsert;
	@Mock PreparedStatement mockPreparedStatement;
	@Mock Session mockSession;
	@Mock Statement mockStatement;
	@Mock Update mockUpdate;
	@Mock Delete mockDelete;

	@Test // DATACASS-202
	public void addPreparedStatementOptionsShouldAddDriverQueryOptions() {

		QueryOptions queryOptions = QueryOptions.builder() //
				.consistencyLevel(ConsistencyLevel.EACH_QUORUM) //
				.retryPolicy(FallthroughRetryPolicy.INSTANCE) //
				.build();

		QueryOptionsUtil.addPreparedStatementOptions(mockPreparedStatement, queryOptions);

		verify(mockPreparedStatement).setConsistencyLevel(ConsistencyLevel.EACH_QUORUM);
		verify(mockPreparedStatement).setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
	}

	@Test // DATACASS-202
	public void addPreparedStatementOptionsShouldAddOurQueryOptions() {

		QueryOptions queryOptions = QueryOptions.builder().retryPolicy(FallthroughRetryPolicy.INSTANCE)
				.consistencyLevel(ConsistencyLevel.LOCAL_QUORUM).build();

		QueryOptionsUtil.addPreparedStatementOptions(mockPreparedStatement, queryOptions);

		verify(mockPreparedStatement).setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
		verify(mockPreparedStatement).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
	}

	@Test // DATACASS-202
	public void addStatementQueryOptionsShouldAddDriverQueryOptions() {

		QueryOptions queryOptions = QueryOptions.builder().consistencyLevel(ConsistencyLevel.EACH_QUORUM) //
				.retryPolicy(FallthroughRetryPolicy.INSTANCE) //
				.build();

		QueryOptionsUtil.addQueryOptions(mockStatement, queryOptions);

		verify(mockStatement).setConsistencyLevel(ConsistencyLevel.EACH_QUORUM);
		verify(mockStatement).setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
	}

	@Test // DATACASS-202
	public void addStatementQueryOptionsShouldAddOurQueryOptions() {

		QueryOptions queryOptions = QueryOptions.builder().retryPolicy(FallthroughRetryPolicy.INSTANCE)
				.consistencyLevel(ConsistencyLevel.LOCAL_QUORUM).build();

		QueryOptionsUtil.addQueryOptions(mockStatement, queryOptions);

		verify(mockStatement).setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
		verify(mockStatement).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
	}

	@Test // DATACASS-202
	public void addStatementQueryOptionsShouldNotAddOptions() {

		QueryOptions queryOptions = QueryOptions.builder().build();

		QueryOptionsUtil.addQueryOptions(mockStatement, queryOptions);

		verifyZeroInteractions(mockStatement);
	}

	@Test // DATACASS-202
	public void addStatementQueryOptionsShouldAddGenericQueryOptions() {

		QueryOptions queryOptions = QueryOptions.builder() //
				.fetchSize(10) //
				.readTimeout(1, TimeUnit.MINUTES) //
				.withTracing() //
				.build();

		QueryOptionsUtil.addQueryOptions(mockStatement, queryOptions);

		verify(mockStatement).setReadTimeoutMillis(60 * 1000);
		verify(mockStatement).setFetchSize(10);
		verify(mockStatement).enableTracing();
	}

	@Test // DATACASS-202
	public void addInsertWriteOptionsShouldAddDriverQueryOptions() {

		WriteOptions writeOptions = WriteOptions.builder() //
				.consistencyLevel(ConsistencyLevel.EACH_QUORUM) //
				.retryPolicy(FallthroughRetryPolicy.INSTANCE) //
				.readTimeout(10) //
				.ttl(10) //
				.build();

		QueryOptionsUtil.addWriteOptions(mockInsert, writeOptions);

		verify(mockInsert).setConsistencyLevel(ConsistencyLevel.EACH_QUORUM);
		verify(mockInsert).setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
		verify(mockInsert).setReadTimeoutMillis(10);
		verify(mockInsert).using(Mockito.any(Using.class));
	}

	@Test // DATACASS-202
	public void addUpdateWriteOptionsShouldAddDriverQueryOptions() {

		WriteOptions writeOptions = WriteOptions.builder() //
				.consistencyLevel(ConsistencyLevel.EACH_QUORUM) //
				.retryPolicy(FallthroughRetryPolicy.INSTANCE) //
				.ttl(10) //
				.tracing(false).build();

		QueryOptionsUtil.addWriteOptions(mockUpdate, writeOptions);

		verify(mockUpdate).setConsistencyLevel(ConsistencyLevel.EACH_QUORUM);
		verify(mockUpdate).setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
		verify(mockUpdate).using(Mockito.any(Using.class));
		verify(mockUpdate).disableTracing();
	}

	@Test // DATACASS-155
	public void addDeleteWriteOptionsShouldAddDriverQueryOptions() {

		WriteOptions writeOptions = WriteOptions.builder() //
				.consistencyLevel(ConsistencyLevel.EACH_QUORUM) //
				.retryPolicy(FallthroughRetryPolicy.INSTANCE) //
				.timestamp(42) //
				.tracing(false).build();

		QueryOptionsUtil.addWriteOptions(mockDelete, writeOptions);

		verify(mockDelete).setConsistencyLevel(ConsistencyLevel.EACH_QUORUM);
		verify(mockDelete).setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
		verify(mockDelete).using(Mockito.any(Using.class));
		verify(mockDelete).disableTracing();
	}
}

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

import java.time.Duration;

import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;

/**
 * Unit tests for {@link QueryOptions}.
 *
 * @author Mark Paluch
 */
public class QueryOptionsUnitTests {

	@Test // DATACASS-202
	public void buildQueryOptions() {

		QueryOptions queryOptions = QueryOptions.builder()
				.consistencyLevel(ConsistencyLevel.ANY)
				.retryPolicy(FallthroughRetryPolicy.INSTANCE)
				.readTimeout(Duration.ofSeconds(1))
				.fetchSize(10)
				.tracing(true)
				.build();

		assertThat(queryOptions.getClass()).isEqualTo(QueryOptions.class);
		assertThat(queryOptions.getRetryPolicy()).isEqualTo(FallthroughRetryPolicy.INSTANCE);
		assertThat(queryOptions.getConsistencyLevel()).isEqualTo(ConsistencyLevel.ANY);
		assertThat(queryOptions.getReadTimeout()).isEqualTo(Duration.ofSeconds(1));
		assertThat(queryOptions.getFetchSize()).isEqualTo(10);
		assertThat(queryOptions.getTracing()).isTrue();
	}

	@Test // DATACASS-202
	public void buildQueryOptionsWithDriverRetryPolicy() {

		QueryOptions writeOptions = QueryOptions.builder()
				.retryPolicy(new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE))
				.build();

		assertThat(writeOptions.getRetryPolicy()).isInstanceOf(LoggingRetryPolicy.class);
	}

	@Test // DATACASS-202
	public void buildQueryOptionsWithRetryPolicy() {

		QueryOptions writeOptions = QueryOptions.builder()
				.retryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
				.build();

		assertThat(writeOptions.getRetryPolicy()).isEqualTo(DowngradingConsistencyRetryPolicy.INSTANCE);
	}

	@Test // DATACASS-56
	public void buildQueryOptionsMutate() {

		QueryOptions queryOptions = QueryOptions.builder()
				.consistencyLevel(ConsistencyLevel.ANY)
				.retryPolicy(FallthroughRetryPolicy.INSTANCE)
				.readTimeout(Duration.ofSeconds(1))
				.fetchSize(10)
				.tracing(true)
				.build();

		QueryOptions mutated = queryOptions.mutate().readTimeout(Duration.ofSeconds(5)).build();

		assertThat(mutated).isNotNull();
		assertThat(mutated).isNotSameAs(queryOptions);
		assertThat(mutated.getClass()).isEqualTo(QueryOptions.class);
		assertThat(mutated.getRetryPolicy()).isEqualTo(FallthroughRetryPolicy.INSTANCE);
		assertThat(mutated.getConsistencyLevel()).isEqualTo(ConsistencyLevel.ANY);
		assertThat(mutated.getReadTimeout()).isEqualTo(Duration.ofSeconds(5));
		assertThat(mutated.getFetchSize()).isEqualTo(10);
		assertThat(mutated.getTracing()).isTrue();
	}
}

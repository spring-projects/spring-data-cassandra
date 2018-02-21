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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;

/**
 * Unit tests for {@link WriteOptions}.
 *
 * @author Mark Paluch
 */
public class WriteOptionsUnitTests {

	@Test // DATACASS-202
	public void buildWriteOptions() {

		WriteOptions writeOptions = WriteOptions.builder()
				.consistencyLevel(com.datastax.driver.core.ConsistencyLevel.ANY)
				.ttl(123)
				.timestamp(1519000753)
				.retryPolicy(FallthroughRetryPolicy.INSTANCE)
				.readTimeout(1)
				.fetchSize(10)
				.withTracing()
				.build();

		assertThat(writeOptions.getTtl()).isEqualTo(Duration.ofSeconds(123));
		assertThat(writeOptions.getTimestamp()).isEqualTo(1519000753);
		assertThat(writeOptions.getRetryPolicy()).isEqualTo(FallthroughRetryPolicy.INSTANCE);
		assertThat(writeOptions.getConsistencyLevel()).isEqualTo(ConsistencyLevel.ANY);
		assertThat(writeOptions.getReadTimeout()).isEqualTo(Duration.ofMillis(1));
		assertThat(writeOptions.getFetchSize()).isEqualTo(10);
		assertThat(writeOptions.getTracing()).isTrue();
	}

	@Test // DATACASS-202
	public void buildReadTimeoutOptionsWriteOptions() {

		WriteOptions writeOptions = WriteOptions.builder().readTimeout(Duration.ofMinutes(1)).build();

		assertThat(writeOptions.getReadTimeout()).isEqualTo(Duration.ofSeconds(60));
		assertThat(writeOptions.getFetchSize()).isNull();
		assertThat(writeOptions.getTracing()).isNull();
	}

	@Test // DATACASS-202
	public void buildQueryOptionsWithDriverRetryPolicy() {

		QueryOptions writeOptions = QueryOptions.builder().retryPolicy(FallthroughRetryPolicy.INSTANCE).build();

		assertThat(writeOptions.getRetryPolicy()).isEqualTo(FallthroughRetryPolicy.INSTANCE);
	}

	@Test // DATACASS-202
	public void buildQueryOptionsWithRetryPolicy() {

		QueryOptions writeOptions = QueryOptions.builder().retryPolicy(FallthroughRetryPolicy.INSTANCE).build();

		assertThat(writeOptions.getRetryPolicy()).isEqualTo(FallthroughRetryPolicy.INSTANCE);
	}

	@Test // DATACASS-56
	public void buildWriteOptionsMutate() {
		Instant now = LocalDateTime.now().toInstant(ZoneOffset.UTC);

		WriteOptions writeOptions = WriteOptions.builder()
				.consistencyLevel(com.datastax.driver.core.ConsistencyLevel.ANY)
				.ttl(123)
				.timestamp(now)
				.retryPolicy(FallthroughRetryPolicy.INSTANCE)
				.readTimeout(1)
				.fetchSize(10)
				.withTracing()
				.build();

		WriteOptions mutated = writeOptions.mutate().retryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE).build();

		assertThat(mutated).isNotNull();
		assertThat(mutated).isNotSameAs(writeOptions);
		assertThat(mutated.getTtl()).isEqualTo(Duration.ofSeconds(123));
		assertThat(mutated.getTimestamp()).isEqualTo(now.toEpochMilli() * 1000);
		assertThat(mutated.getRetryPolicy()).isEqualTo(DowngradingConsistencyRetryPolicy.INSTANCE);
		assertThat(mutated.getConsistencyLevel()).isEqualTo(ConsistencyLevel.ANY);
		assertThat(mutated.getReadTimeout()).isEqualTo(Duration.ofMillis(1));
		assertThat(mutated.getFetchSize()).isEqualTo(10);
		assertThat(mutated.getTracing()).isTrue();
	}
}

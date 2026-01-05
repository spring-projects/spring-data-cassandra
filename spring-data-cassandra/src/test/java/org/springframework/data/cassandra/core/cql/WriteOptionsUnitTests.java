/*
 * Copyright 2016-present the original author or authors.
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

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;

/**
 * Unit tests for {@link WriteOptions}.
 *
 * @author Mark Paluch
 * @author Sam Lightfoot
 * @author Thomas Strau&szlig;
 * @author Tudor Marc
 */
class WriteOptionsUnitTests {

	@Test // DATACASS-202, DATACASS-767, GH-1220
	void buildWriteOptions() {

		WriteOptions writeOptions = WriteOptions.builder() //
				.consistencyLevel(DefaultConsistencyLevel.ANY) //
				.ttl(123) //
				.timestamp(1519000753) //
				.readTimeout(1) //
				.pageSize(10) //
				.withTracing() //
				.keyspace(CqlIdentifier.fromCql("my_keyspace")) //
				.idempotent(true) //
				.routingKeyspace(CqlIdentifier.fromCql("routing_keyspace")) //
				.routingKey(ByteBuffer.allocate(1)) //
				.build();

		assertThat(writeOptions.getTtl()).isEqualTo(Duration.ofSeconds(123));
		assertThat(writeOptions.getTimestamp()).isEqualTo(1519000753);
		assertThat(writeOptions.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.ANY);
		assertThat(writeOptions.getTimeout()).isEqualTo(Duration.ofMillis(1));
		assertThat(writeOptions.getPageSize()).isEqualTo(10);
		assertThat(writeOptions.getTracing()).isTrue();
		assertThat(writeOptions.getKeyspace()).isEqualTo(CqlIdentifier.fromCql("my_keyspace"));
		assertThat(writeOptions.isIdempotent()).isEqualTo(true);
		assertThat(writeOptions.getRoutingKeyspace()).isEqualTo(CqlIdentifier.fromCql("routing_keyspace"));
		assertThat(writeOptions.getRoutingKey()).isEqualTo(ByteBuffer.allocate(1));
	}

	@Test // DATACASS-202
	void buildReadTimeoutOptionsWriteOptions() {

		WriteOptions writeOptions = WriteOptions.builder().timeout(Duration.ofMinutes(1)).build();

		assertThat(writeOptions.getTimeout()).isEqualTo(Duration.ofSeconds(60));
		assertThat(writeOptions.getPageSize()).isNull();
		assertThat(writeOptions.getTracing()).isNull();
	}

	@Test // DATACASS-56, GH-1220
	void buildWriteOptionsMutate() {
		Instant now = LocalDateTime.now().toInstant(ZoneOffset.UTC);

		WriteOptions writeOptions = WriteOptions.builder() //
				.consistencyLevel(DefaultConsistencyLevel.ANY) //
				.ttl(123) //
				.timestamp(now) //
				.readTimeout(1) //
				.pageSize(10) //
				.withTracing() //
				.idempotent(true) //
				.routingKeyspace(CqlIdentifier.fromCql("routing_keyspace")) //
				.routingKey(ByteBuffer.allocate(1)) //
				.build();

		WriteOptions mutated = writeOptions.mutate().timeout(Duration.ofMillis(100)).build();

		assertThat(mutated).isNotNull();
		assertThat(mutated).isNotSameAs(writeOptions);
		assertThat(mutated.getTtl()).isEqualTo(Duration.ofSeconds(123));
		assertThat(mutated.getTimestamp()).isEqualTo(now.toEpochMilli() * 1000);
		assertThat(mutated.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.ANY);
		assertThat(mutated.getTimeout()).isEqualTo(Duration.ofMillis(100));
		assertThat(mutated.getPageSize()).isEqualTo(10);
		assertThat(mutated.getTracing()).isTrue();
		assertThat(writeOptions.isIdempotent()).isEqualTo(true);
		assertThat(writeOptions.getRoutingKeyspace()).isEqualTo(CqlIdentifier.fromCql("routing_keyspace"));
		assertThat(writeOptions.getRoutingKey()).isEqualTo(ByteBuffer.allocate(1));
	}

	@Test // GH-1248
	void buildWriteOptionsWithTtlNegativeDuration() {
		assertThatIllegalArgumentException().isThrownBy(() -> WriteOptions.builder().ttl(-1));
		assertThatIllegalArgumentException()
				.isThrownBy(() -> WriteOptions.builder().ttl(Duration.of(-1, ChronoUnit.MICROS)));
	}

	@Test // GH-1262
	void buildZeroDurationTtlWriterOptions() {

		WriteOptions writeOptions = WriteOptions.builder().ttl(0).build();

		assertThat(writeOptions.getTtl()).isEqualTo(Duration.ZERO);
	}
}

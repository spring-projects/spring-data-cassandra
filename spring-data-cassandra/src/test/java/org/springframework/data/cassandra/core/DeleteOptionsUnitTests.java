/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.cassandra.core;

import static org.assertj.core.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.query.Query;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;

/**
 * Unit tests for {@link DeleteOptions}.
 *
 * @author Mark Paluch
 */
class DeleteOptionsUnitTests {

	@Test // DATACASS-575, DATACASS-708, DATACASS-767
	void shouldConfigureDeleteOptions() {

		Instant now = Instant.ofEpochSecond(1234);

		DeleteOptions deleteOptions = DeleteOptions.builder() //
				.ttl(10) //
				.timestamp(now) //
				.withIfExists() //
				.executionProfile("foo") //
				.serialConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE) //
				.keyspace(CqlIdentifier.fromCql("my_keyspace"))
				.build();

		assertThat(deleteOptions.getTtl()).isEqualTo(Duration.ofSeconds(10));
		assertThat(deleteOptions.getTimestamp()).isEqualTo(now.toEpochMilli() * 1000);
		assertThat(deleteOptions.isIfExists()).isTrue();
		assertThat(deleteOptions.getIfCondition()).isNull();
		assertThat(deleteOptions.getKeyspace()).isEqualTo(CqlIdentifier.fromCql("my_keyspace"));
	}

	@Test // DATACASS-575
	void buildDeleteOptionsMutate() {

		DeleteOptions deleteOptions = DeleteOptions.builder() //
				.ttl(10) //
				.timestamp(1519222753) //
				.withIfExists() //
				.build();

		DeleteOptions mutated = deleteOptions.mutate().ttl(20).timestamp(1519000753).build();

		assertThat(mutated).isNotNull();
		assertThat(mutated).isNotSameAs(deleteOptions);
		assertThat(mutated.getTtl()).isEqualTo(Duration.ofSeconds(20));
		assertThat(mutated.getTimestamp()).isEqualTo(1519000753);
		assertThat(mutated.isIfExists()).isTrue();
		assertThat(mutated.getIfCondition()).isNull();
	}

	@Test // DATACASS-575
	void shouldApplyFilterCondition() {

		DeleteOptions deleteOptions = DeleteOptions.builder() //
				.withIfExists() //
				.ifCondition(Query.empty()) //
				.build();

		assertThat(deleteOptions.isIfExists()).isFalse();
		assertThat(deleteOptions.getIfCondition()).isEqualTo(Query.empty());
	}
}

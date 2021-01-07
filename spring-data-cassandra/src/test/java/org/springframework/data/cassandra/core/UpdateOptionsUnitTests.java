/*
 * Copyright 2017-2021 the original author or authors.
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

import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.query.Query;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;

/**
 * Unit tests for {@link UpdateOptions}.
 *
 * @author Mark Paluch
 * @author Lukasz Antoniak
 */
class UpdateOptionsUnitTests {

	@Test // DATACASS-250, DATACASS-155, DATACASS-708, DATACASS-767
	void shouldConfigureUpdateOptions() {

		Instant now = Instant.ofEpochSecond(1234);

		UpdateOptions updateOptions = UpdateOptions.builder() //
				.ttl(10) //
				.timestamp(now) //
				.executionProfile("foo") //
				.serialConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE) //
				.withIfExists() //
				.keyspace(CqlIdentifier.fromCql("my_keyspace")) //
				.build();

		assertThat(updateOptions.getTtl()).isEqualTo(Duration.ofSeconds(10));
		assertThat(updateOptions.getTimestamp()).isEqualTo(now.toEpochMilli() * 1000);
		assertThat(updateOptions.isIfExists()).isTrue();
		assertThat(updateOptions.getIfCondition()).isNull();
		assertThat(updateOptions.getKeyspace()).isEqualTo(CqlIdentifier.fromCql("my_keyspace"));
	}

	@Test // DATACASS-56, DATACASS-155
	void buildUpdateOptionsMutate() {

		UpdateOptions updateOptions = UpdateOptions.builder() //
				.ttl(10) //
				.timestamp(1519222753) //
				.withIfExists() //
				.build();

		UpdateOptions mutated = updateOptions.mutate() //
				.ttl(20) //
				.timestamp(1519000753) //
				.build();

		assertThat(mutated).isNotNull();
		assertThat(mutated).isNotSameAs(updateOptions);
		assertThat(mutated.getTtl()).isEqualTo(Duration.ofSeconds(20));
		assertThat(mutated.getTimestamp()).isEqualTo(1519000753);
		assertThat(mutated.isIfExists()).isTrue();
		assertThat(mutated.getIfCondition()).isNull();
	}

	@Test // DATACASS-575
	void shouldApplyFilterCondition() {

		UpdateOptions updateOptions = UpdateOptions.builder() //
				.withIfExists() //
				.ifCondition(Query.empty()) //
				.build();

		assertThat(updateOptions.isIfExists()).isFalse();
		assertThat(updateOptions.getIfCondition()).isEqualTo(Query.empty());
	}
}

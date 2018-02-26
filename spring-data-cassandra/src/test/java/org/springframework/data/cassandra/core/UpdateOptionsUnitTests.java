/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.cassandra.core;

import static org.assertj.core.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;

import org.junit.Test;

/**
 * Unit tests for {@link UpdateOptions}.
 *
 * @author Mark Paluch
 * @author Lukasz Antoniak
 */
public class UpdateOptionsUnitTests {

	@Test // DATACASS-250, DATACASS-155
	public void shouldConfigureUpdateOptions() {

		Instant now = Instant.ofEpochSecond(1234);

		UpdateOptions updateOptions = UpdateOptions.builder()
				.ttl(10)
				.timestamp(now)
				.withIfExists()
				.build();

		assertThat(updateOptions.getTtl()).isEqualTo(Duration.ofSeconds(10));
		assertThat(updateOptions.getTimestamp()).isEqualTo(now.toEpochMilli() * 1000);
		assertThat(updateOptions.isIfExists()).isTrue();
	}

	@Test // DATACASS-56, DATACASS-155
	public void buildUpdateOptionsMutate() {

		UpdateOptions updateOptions = UpdateOptions.builder()
				.ttl(10)
				.timestamp(1519222753)
				.withIfExists()
				.build();

		UpdateOptions mutated = updateOptions.mutate().ttl(20).timestamp(1519000753).build();

		assertThat(mutated).isNotNull();
		assertThat(mutated).isNotSameAs(updateOptions);
		assertThat(mutated.getTtl()).isEqualTo(Duration.ofSeconds(20));
		assertThat(mutated.getTimestamp()).isEqualTo(1519000753);
		assertThat(mutated.isIfExists()).isTrue();
	}
}

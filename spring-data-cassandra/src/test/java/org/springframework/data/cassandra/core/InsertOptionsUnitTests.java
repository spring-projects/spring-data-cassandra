/*
 * Copyright 2017 the original author or authors.
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

import org.junit.Test;

/**
 * Unit tests for {@link InsertOptions}.
 *
 * @author Mark Paluch
 */
public class InsertOptionsUnitTests {

	@Test // DATACASS-250
	public void shouldConfigureInsertOptions() {

		InsertOptions insertOptions = InsertOptions.builder() //
				.ttl(10) //
				.withIfNotExists() //
				.build();

		assertThat(insertOptions.getTtl()).isEqualTo(Duration.ofSeconds(10));
		assertThat(insertOptions.isIfNotExists()).isTrue();
	}

	@Test // DATACASS-56
	public void buildInsertOptionsMutate() {

		InsertOptions insertOptions = InsertOptions.builder() //
				.ttl(10) //
				.withIfNotExists() //
				.build();

		InsertOptions mutated = insertOptions.mutate().ttl(Duration.ofSeconds(5)).build();

		assertThat(mutated.getTtl()).isEqualTo(Duration.ofSeconds(5));
		assertThat(mutated.isIfNotExists()).isTrue();
	}
}

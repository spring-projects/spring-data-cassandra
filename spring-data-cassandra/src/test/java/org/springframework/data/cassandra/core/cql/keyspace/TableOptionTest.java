/*
 * Copyright 2016-2025 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.keyspace;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TableOption}.
 *
 * @author Seungho Kang
 */
class TableOptionTest {

	@Test // GH-1584
	void shouldResolveTableOptionUsingValueOfIgnoreCase() {

		TableOption option = TableOption.valueOfIgnoreCase("bloom_filter_fp_chance");

		assertThat(option).isEqualTo(TableOption.BLOOM_FILTER_FP_CHANCE);
	}

	@Test // GH-1584
	void shouldThrowExceptionWhenTableOptionNotFoundInValueOfIgnoreCase() {

		assertThatThrownBy(() -> TableOption.valueOfIgnoreCase("unknown_option"))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("Unable to recognize specified Table option");
	}

	@Test // GH-1584
	void shouldResolveKnownTableOptionByName() {

		TableOption tableOption = TableOption.findByNameIgnoreCase("bloom_filter_fp_chance");

		assertThat(tableOption).isEqualTo(TableOption.BLOOM_FILTER_FP_CHANCE);
	}

	@Test // GH-1584
	void shouldReturnNullForUnknownTableOption() {

		TableOption tableOption = TableOption.findByNameIgnoreCase("unknown_option");

		assertThat(tableOption).isNull();
	}

}

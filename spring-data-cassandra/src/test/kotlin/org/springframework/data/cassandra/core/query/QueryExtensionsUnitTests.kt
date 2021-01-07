/*
 * Copyright 2018-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.query

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Unit tests for [QueryExtensions].
 *
 * @author Mark Paluch
 */
class QueryExtensionsUnitTests {

	@Test // DATACASS-595
	fun `should create query from single criteria`() {

		val query = query(where("jedi").isEqualTo(true))

		assertThat(query.toString()).contains("jedi = true")
	}

	@Test // DATACASS-484
	fun `should create query from concatenated criteria`() {

		val query = query(where("foo").isEqualTo("bar") and //
				where("baz").isEqualTo("bar") and //
				where("name").isEqualTo("Doe"))

		assertThat(query.toString()).contains("foo = 'bar' AND baz = 'bar' AND name = 'Doe'")
	}
}


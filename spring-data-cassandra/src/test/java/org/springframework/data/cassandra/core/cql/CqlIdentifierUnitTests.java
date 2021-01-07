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
package org.springframework.data.cassandra.core.cql;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.cassandra.core.cql.CqlIdentifier.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CqlIdentifier}.
 *
 * @author John McPeek
 * @author Matthew T. Adams
 */
class CqlIdentifierUnitTests {

	@Test
	void testUnquotedIdentifiers() {

		String[] ids = new String[] { "foo", "Foo", "FOO", "a_", "a1" };

		for (String id : ids) {
			CqlIdentifier cqlId = of(id);
			assertThat(cqlId.isQuoted()).isFalse();
			assertThat(cqlId.toCql()).isEqualTo(id.toLowerCase());
		}
	}

	@Test
	void testForceQuotedIdentifiers() {

		String[] ids = new String[] { "foo", "Foo", "FOO", "a_", "a1" };

		for (String id : ids) {
			CqlIdentifier cqlId = quoted(id);
			assertThat(cqlId.isQuoted()).isTrue();
			assertThat(cqlId.toCql()).isEqualTo("\"" + id + "\"");
		}
	}

	@Test
	void testReservedWordsEndUpQuoted() {

		for (ReservedKeyword id : ReservedKeyword.values()) {
			CqlIdentifier cqlId = of(id.name());
			assertThat(cqlId.isQuoted()).isTrue();
			assertThat(cqlId.toCql()).isEqualTo("\"" + id.name() + "\"");

			cqlId = of(id.name().toLowerCase());
			assertThat(cqlId.isQuoted()).isTrue();
			assertThat(cqlId.toCql()).isEqualTo("\"" + id.name().toLowerCase() + "\"");
		}
	}

	@Test
	void testIllegals() {
		String[] illegals = new String[] { null, "", "a ", "a a", "a\"", "a'", "a''", "\"\"", "''", "-", "a-", "_", "_a" };
		for (String illegal : illegals) {
			try {
				of(illegal);
				fail(String.format("identifier [%s] should have caused IllegalArgumentException", illegal));
			} catch (IllegalArgumentException x) {
				// :)
			}
		}
	}
}

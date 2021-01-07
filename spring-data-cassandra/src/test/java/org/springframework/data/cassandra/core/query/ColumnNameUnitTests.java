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
package org.springframework.data.cassandra.core.query;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Unit tests for {@link ColumnName}.
 *
 * @author Mark Paluch
 */
class ColumnNameUnitTests {

	@Test // DATACASS-343
	void stringBasedShouldEqual() {

		ColumnName first = ColumnName.from("foo");
		ColumnName second = ColumnName.from("foo");
		ColumnName different = ColumnName.from("Foo");

		assertThat(first).isEqualTo(second);
		assertThat(first.equals(second)).isTrue();
		assertThat(first.hashCode()).isEqualTo(second.hashCode());

		assertThat(first).isNotEqualTo(different);
		assertThat(first.equals(different)).isFalse();
		assertThat(first.hashCode()).isNotEqualTo(different.hashCode());
	}

	@Test // DATACASS-343
	void cqlBasedShouldEqual() {

		ColumnName first = ColumnName.from(CqlIdentifier.fromCql("foo"));
		ColumnName second = ColumnName.from(CqlIdentifier.fromCql("Foo"));

		ColumnName different = ColumnName.from(CqlIdentifier.fromInternal("bar"));

		assertThat(first).isEqualTo(second);
		assertThat(first.equals(second)).isTrue();
		assertThat(first.hashCode()).isEqualTo(second.hashCode());

		assertThat(first).isNotEqualTo(different);
		assertThat(first.equals(different)).isFalse();
		assertThat(first.hashCode()).isNotEqualTo(different.hashCode());
	}

	@Test // DATACASS-343
	void stringAndCqlComparisonShouldEqual() {

		ColumnName first = ColumnName.from("foo");
		ColumnName second = ColumnName.from(CqlIdentifier.fromCql("foo"));
		ColumnName different = ColumnName.from(CqlIdentifier.fromCql("one"));

		assertThat(first).isEqualTo(second);
		assertThat(first.equals(second)).isTrue();
		assertThat(first.hashCode()).isEqualTo(second.hashCode());

		assertThat(first).isNotEqualTo(different);
		assertThat(first.equals(different)).isFalse();
		assertThat(first.hashCode()).isNotEqualTo(different.hashCode());
	}
}

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
package org.springframework.data.cassandra.core.query;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;

/**
 * Unit tests for {@link ColumnName}.
 *
 * @author Mark Paluch
 */
public class ColumnNameUnitTests {

	@Test // DATACASS-343
	public void stringBasedShouldEqual() {

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
	public void cqlBasedShouldEqual() {

		ColumnName first = ColumnName.from(CqlIdentifier.of("foo"));
		ColumnName second = ColumnName.from(CqlIdentifier.of("Foo"));

		ColumnName different = ColumnName.from(CqlIdentifier.of("Foo", true));

		assertThat(first).isEqualTo(second);
		assertThat(first.equals(second)).isTrue();
		assertThat(first.hashCode()).isEqualTo(second.hashCode());

		assertThat(first).isNotEqualTo(different);
		assertThat(first.equals(different)).isFalse();
		assertThat(first.hashCode()).isNotEqualTo(different.hashCode());
	}

	@Test // DATACASS-343
	public void stringAndCqlComparisonShouldEqual() {

		ColumnName first = ColumnName.from("foo");
		ColumnName second = ColumnName.from(CqlIdentifier.of("foo"));
		ColumnName different = ColumnName.from(CqlIdentifier.of("one", true));

		assertThat(first).isEqualTo(second);
		assertThat(first.equals(second)).isTrue();
		assertThat(first.hashCode()).isEqualTo(second.hashCode());

		assertThat(first).isNotEqualTo(different);
		assertThat(first.equals(different)).isFalse();
		assertThat(first.hashCode()).isNotEqualTo(different.hashCode());
	}
}

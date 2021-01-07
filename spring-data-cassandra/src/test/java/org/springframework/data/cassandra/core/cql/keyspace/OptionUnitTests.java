/*
 * Copyright 2016-2021 the original author or authors.
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

import java.lang.annotation.RetentionPolicy;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Option}.
 *
 * @author Matthew T. Adams
 * @author JohnMcPeek
 */
class OptionUnitTests {

	@Test
	void testOptionWithNullName() {
		assertThatIllegalArgumentException().isThrownBy(() -> new DefaultOption(null, Object.class, true, true, true));
	}

	@Test
	void testOptionWithEmptyName() {
		assertThatIllegalArgumentException().isThrownBy(() -> new DefaultOption("", Object.class, true, true, true));
	}

	@Test
	void testOptionWithNullType() {
		new DefaultOption("opt", Void.class, true, true, true);
		new DefaultOption("opt", Void.class, false, true, true);
	}

	@Test
	void testOptionWithNullTypeIsCoerceable() {
		Option op = new DefaultOption("opt", Void.class, true, true, true);
		assertThat(op.isCoerceable("")).isTrue();
	}

	@Test
	void testOptionValueCoercion() {

		String name = "my_option";
		Class<?> type = String.class;
		boolean requires = true;
		boolean escapes = true;
		boolean quotes = true;

		Option op = new DefaultOption(name, type, requires, escapes, quotes);

		assertThat(op.isCoerceable("opt")).isTrue();
		assertThat(op.toString("opt")).isEqualTo("'opt'");
		assertThat(op.toString("opt'n")).isEqualTo("'opt''n'");

		type = Long.class;
		escapes = false;
		quotes = false;
		op = new DefaultOption(name, type, requires, escapes, quotes);

		String expected = "1";
		for (Object value : new Object[] { 1, "1" }) {
			assertThat(op.isCoerceable(value)).isTrue();
			assertThat(op.toString(value)).isEqualTo(expected);
		}
		assertThat(op.isCoerceable("x")).isFalse();

		type = Long.class;
		escapes = false;
		quotes = true;
		op = new DefaultOption(name, type, requires, escapes, quotes);

		expected = "'1'";
		for (Object value : new Object[] { 1, "1" }) {
			assertThat(op.isCoerceable(value)).isTrue();
			assertThat(op.toString(value)).isEqualTo(expected);
		}
		assertThat(op.isCoerceable("x")).isFalse();

		type = Double.class;
		escapes = false;
		quotes = false;
		op = new DefaultOption(name, type, requires, escapes, quotes);

		String[] expecteds = new String[] { "1", "1.0", "1.0", "1", "1.0" };
		Object[] values = new Object[] { 1, 1.0F, 1.0D, "1", "1.0" };
		for (int i = 0; i < values.length; i++) {
			assertThat(op.isCoerceable(values[i])).isTrue();
			assertThat(op.toString(values[i])).isEqualTo(expecteds[i]);
		}
		assertThat(op.isCoerceable("x")).isFalse();

		type = RetentionPolicy.class;
		escapes = false;
		quotes = false;
		op = new DefaultOption(name, type, requires, escapes, quotes);

		assertThat(op.isCoerceable(RetentionPolicy.CLASS)).isTrue();
		assertThat(op.isCoerceable("CLASS")).isTrue();
		assertThat(op.isCoerceable("x")).isFalse();
		assertThat(op.toString("CLASS")).isEqualTo("CLASS");
		assertThat(op.toString(RetentionPolicy.CLASS)).isEqualTo("CLASS");
	}
}

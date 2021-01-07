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
import org.springframework.data.cassandra.core.query.Update.IncrOp;

/**
 * Unit tests for {@link Update}.
 *
 * @author Mark Paluch
 * @author Chema Vinacua
 */
class UpdateUnitTests {

	@Test // DATACASS-343
	void shouldCreateSimpleUpdate() {

		Update update = Update.update("foo", "bar");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("foo = 'bar'");
	}

	@Test // DATACASS-343
	void shouldCreateSetAtIndexUpdate() {

		Update update = Update.empty().set("foo").atIndex(10).to("bar");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("foo[10] = 'bar'");
	}

	@Test // DATACASS-343
	void shouldCreateSetAtKeyUpdate() {

		Update update = Update.empty().set("foo").atKey("baz").to("bar");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("foo['baz'] = 'bar'");
	}

	@Test // DATACASS-343
	void shouldAddToMap() {

		Update update = Update.empty().addTo("foo").entry("foo", "bar");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("foo = foo + {'foo':'bar'}");
	}

	@Test // DATACASS-343
	void shouldPrependAllToList() {

		Update update = Update.empty().addTo("foo").prependAll("foo", "bar");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("foo = ['foo','bar'] + foo");
	}

	@Test // DATACASS-343
	void shouldAppendAllToList() {

		Update update = Update.empty().addTo("foo").appendAll("foo", "bar");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("foo = foo + ['foo','bar']");
	}

	@Test // DATACASS-343
	void shouldRemoveFromList() {

		Update update = Update.empty().remove("foo", "bar");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("foo = foo - ['bar']");
	}

	@Test // DATACASS-343
	void shouldClearCollection() {

		Update update = Update.empty().clear("foo");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("foo = []");
	}

	@Test // DATACASS-343
	void shouldCreateIncrementUpdate() {

		Update update = Update.empty().increment("foo");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("foo = foo + 1");
	}

	@Test // DATACASS-343
	void shouldCreateDecrementUpdate() {

		Update update = Update.empty().decrement("foo");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("foo = foo - 1");
	}

	@Test // DATACASS-343
	void shouldCreateUpdateForTwoColumns() {

		Update update = Update.empty().increment("foo").decrement("bar");

		assertThat(update.getUpdateOperations()).hasSize(2);
		assertThat(update).hasToString("foo = foo + 1, bar = bar - 1");
	}

	@Test // DATACASS-343
	void shouldCreateSingleUpdateForTheSameColumn() {

		Update update = Update.empty().set("foo", "bar").decrement("foo");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.getUpdateOperations().iterator().next()).isInstanceOf(IncrOp.class);
	}

	@Test // DATACASS-718
	void shouldCreateIncrementLongUpdate() {

		Update update = Update.empty().increment("foo", 2400000000L);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("foo = foo + 2400000000");
	}

	@Test // DATACASS-718
	void shouldCreateDecrementLongUpdate() {

		Update update = Update.empty().decrement("foo", 2400000000L);

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update).hasToString("foo = foo - 2400000000");
	}
}

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
import org.springframework.data.cassandra.core.query.Update.IncrOp;

/**
 * Unit tests for {@link Update}.
 *
 * @author Mark Paluch
 */
public class UpdateUnitTests {

	@Test // DATACASS-343
	public void shouldCreateSimpleUpdate() {

		Update update = Update.update("foo", "bar");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("foo = 'bar'");
	}

	@Test // DATACASS-343
	public void shouldCreateSetAtIndexUpdate() {

		Update update = Update.empty().set("foo").atIndex(10).to("bar");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("foo[10] = 'bar'");
	}

	@Test // DATACASS-343
	public void shouldCreateSetAtKeyUpdate() {

		Update update = Update.empty().set("foo").atKey("baz").to("bar");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("foo['baz'] = 'bar'");
	}

	@Test // DATACASS-343
	public void shouldAddToMap() {

		Update update = Update.empty().addTo("foo").entry("foo", "bar");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("foo = foo + {'foo':'bar'}");
	}

	@Test // DATACASS-343
	public void shouldPrependAllToList() {

		Update update = Update.empty().addTo("foo").prependAll("foo", "bar");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("foo = ['foo','bar'] + foo");
	}

	@Test // DATACASS-343
	public void shouldAppendAllToList() {

		Update update = Update.empty().addTo("foo").appendAll("foo", "bar");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("foo = foo + ['foo','bar']");
	}

	@Test // DATACASS-343
	public void shouldRemoveFromList() {

		Update update = Update.empty().remove("foo", "bar");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("foo = foo - ['bar']");
	}

	@Test // DATACASS-343
	public void shouldClearCollection() {

		Update update = Update.empty().clear("foo");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("foo = []");
	}

	@Test // DATACASS-343
	public void shouldCreateIncrementUpdate() {

		Update update = Update.empty().increment("foo");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("foo = foo + 1");
	}

	@Test // DATACASS-343
	public void shouldCreateDecrementUpdate() {

		Update update = Update.empty().decrement("foo");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.toString()).isEqualTo("foo = foo - 1");
	}

	@Test // DATACASS-343
	public void shouldCreateUpdateForTwoColumns() {

		Update update = Update.empty().increment("foo").decrement("bar");

		assertThat(update.getUpdateOperations()).hasSize(2);
		assertThat(update.toString()).isEqualTo("foo = foo + 1, bar = bar - 1");
	}

	@Test // DATACASS-343
	public void shouldCreateSingleUpdateForTheSameColumn() {

		Update update = Update.empty().set("foo", "bar").decrement("foo");

		assertThat(update.getUpdateOperations()).hasSize(1);
		assertThat(update.getUpdateOperations().iterator().next()).isInstanceOf(IncrOp.class);
	}
}

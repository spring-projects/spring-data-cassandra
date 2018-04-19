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
import static org.springframework.data.cassandra.core.query.SerializationUtils.*;

import java.util.Arrays;
import java.util.HashSet;

import org.junit.Test;

/**
 * Unit tests for {@link Criteria}.
 *
 * @author Mark Paluch
 */
public class CriteriaUnitTests {

	@Test // DATACASS-343
	public void shouldCreateIsEqualTo() {

		CriteriaDefinition criteria = Criteria.where("foo").is("bar");

		assertThat(serializeToCqlSafely(criteria)).isEqualTo("foo = 'bar'");
	}

	@Test // DATACASS-549
	public void shouldCreateIsNotEqualTo() {

		CriteriaDefinition criteria = Criteria.where("foo").ne("bar");

		assertThat(serializeToCqlSafely(criteria)).isEqualTo("foo != 'bar'");
	}

	@Test // DATACASS-549
	public void shouldCreateIsNotNull() {

		CriteriaDefinition criteria = Criteria.where("foo").isNotNull();

		assertThat(serializeToCqlSafely(criteria)).isEqualTo("foo IS NOT NULL");
	}

	@Test // DATACASS-343
	public void shouldCreateIsGreater() {

		CriteriaDefinition criteria = Criteria.where("foo").gt(17);

		assertThat(serializeToCqlSafely(criteria)).isEqualTo("foo > 17");
	}

	@Test // DATACASS-343
	public void shouldCreateIsGreaterOrEquals() {

		CriteriaDefinition criteria = Criteria.where("foo").gte(17);

		assertThat(serializeToCqlSafely(criteria)).isEqualTo("foo >= 17");
	}

	@Test // DATACASS-343
	public void shouldCreateIsLess() {

		CriteriaDefinition criteria = Criteria.where("foo").lt(17);

		assertThat(serializeToCqlSafely(criteria)).isEqualTo("foo < 17");
	}

	@Test // DATACASS-343
	public void shouldCreateIsLessOrEquals() {

		CriteriaDefinition criteria = Criteria.where("foo").lte(17);

		assertThat(serializeToCqlSafely(criteria)).isEqualTo("foo <= 17");
	}

	@Test // DATACASS-343
	public void shouldCreateIsInArray() {

		CriteriaDefinition criteria = Criteria.where("foo").in("a", "b", "c");

		assertThat(serializeToCqlSafely(criteria)).isEqualTo("foo IN ['a','b','c']");
	}

	@Test // DATACASS-343
	public void shouldCreateIsInCollection() {

		CriteriaDefinition criteria = Criteria.where("foo").in(Arrays.asList("a", "b", "c"));

		assertThat(serializeToCqlSafely(criteria)).isEqualTo("foo IN ['a','b','c']");
	}

	@Test // DATACASS-343
	public void shouldCreateIsInListOfObject() {

		CriteriaDefinition criteria = Criteria.where("foo").in(Arrays.asList("a", "b", new Object()));

		assertThat(serializeToCqlSafely(criteria)).startsWith("foo IN ['a','b',java.lang.Object@");
	}

	@Test // DATACASS-343
	public void shouldCreateIsInSet() {

		CriteriaDefinition criteria = Criteria.where("foo").in(new HashSet<>(Arrays.asList("a", "b", "c")));

		assertThat(serializeToCqlSafely(criteria)).isEqualTo("foo IN {'a','b','c'}");
	}

	@Test // DATACASS-343
	public void shouldCreateLike() {

		CriteriaDefinition criteria = Criteria.where("foo").like("a%");

		assertThat(serializeToCqlSafely(criteria)).isEqualTo("foo LIKE 'a%'");
	}

	@Test // DATACASS-343
	public void shouldCreateContains() {

		CriteriaDefinition criteria = Criteria.where("foo").contains("a");

		assertThat(serializeToCqlSafely(criteria)).isEqualTo("foo CONTAINS 'a'");
	}

	@Test // DATACASS-343
	public void shouldCreateContainsKey() {

		CriteriaDefinition criteria = Criteria.where("foo").containsKey("a");

		assertThat(serializeToCqlSafely(criteria)).isEqualTo("foo CONTAINS KEY 'a'");
	}
}

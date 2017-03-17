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
package org.springframework.data.cassandra.core.query;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.cassandra.core.query.SerializationUtils.*;

import java.util.List;

import org.junit.Test;

/**
 * Unit tests for {@link ChainedCriteria}.
 *
 * @author Mark Paluch
 */
@SuppressWarnings("unchecked")
public class ChainedCriteriaUnitTests {

	@Test // DATACASS-343
	public void shouldChain() {

		ChainedCriteria criteria = ChainedCriteria.where("foo").is("bar").and("baz").gt(17);

		List<Criteria> chain = (List) criteria.getCriteriaDefinitions();

		assertThat(serializeToCqlSafely(chain.get(0))).isEqualTo("foo = 'bar'");
		assertThat(serializeToCqlSafely(chain.get(1))).isEqualTo("baz > 17");
	}

	@Test // DATACASS-343
	public void shouldCreateCriteriaChainFromCrirteria() {

		ChainedCriteria criteria = ChainedCriteria.from(Criteria.where("foo").is("bar"), Criteria.where("baz").gt(17));

		List<Criteria> chain = (List) criteria.getCriteriaDefinitions();

		assertThat(serializeToCqlSafely(chain.get(0))).isEqualTo("foo = 'bar'");
		assertThat(serializeToCqlSafely(chain.get(1))).isEqualTo("baz > 17");
	}
}

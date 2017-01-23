/*
 * Copyright 2016-2017 the original author or authors.
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
package org.springframework.cassandra.core;

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Unit tests for {@link ConsistencyLevelResolver}.
 *
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class ConsistencyLevelResolverUnitTests {

	private final ConsistencyLevel from;
	private final com.datastax.driver.core.ConsistencyLevel expected;

	public ConsistencyLevelResolverUnitTests(ConsistencyLevel from, com.datastax.driver.core.ConsistencyLevel expected) {

		this.from = from;
		this.expected = expected;
	}

	@Parameters(name = "{0}")
	public static List<Object[]> parameters() {

		Map<ConsistencyLevel, com.datastax.driver.core.ConsistencyLevel> expectations = new LinkedHashMap<>();

		expectations.put(ConsistencyLevel.ALL, com.datastax.driver.core.ConsistencyLevel.ALL);
		expectations.put(ConsistencyLevel.ANY, com.datastax.driver.core.ConsistencyLevel.ANY);

		expectations.put(ConsistencyLevel.QUOROM, com.datastax.driver.core.ConsistencyLevel.QUORUM);
		expectations.put(ConsistencyLevel.QUORUM, com.datastax.driver.core.ConsistencyLevel.QUORUM);

		expectations.put(ConsistencyLevel.LOCAL_QUOROM, com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM);
		expectations.put(ConsistencyLevel.LOCAL_QUORUM, com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM);

		expectations.put(ConsistencyLevel.EACH_QUOROM, com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM);
		expectations.put(ConsistencyLevel.EACH_QUORUM, com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM);

		expectations.put(ConsistencyLevel.LOCAL_ONE, com.datastax.driver.core.ConsistencyLevel.LOCAL_ONE);
		expectations.put(ConsistencyLevel.LOCAL_SERIAL, com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL);
		expectations.put(ConsistencyLevel.SERIAL, com.datastax.driver.core.ConsistencyLevel.SERIAL);

		expectations.put(ConsistencyLevel.ONE, com.datastax.driver.core.ConsistencyLevel.ONE);
		expectations.put(ConsistencyLevel.TWO, com.datastax.driver.core.ConsistencyLevel.TWO);
		expectations.put(ConsistencyLevel.THREE, com.datastax.driver.core.ConsistencyLevel.THREE);

		List<Object[]> parameters = new ArrayList<>();

		for (Entry<ConsistencyLevel, com.datastax.driver.core.ConsistencyLevel> entry : expectations.entrySet()) {
			parameters.add(new Object[] { entry.getKey(), entry.getValue() });
		}

		return parameters;
	}

	@Test // DATACASS-202
	public void shouldResolveCorrectly() {
		assertThat(ConsistencyLevelResolver.resolve(from)).isEqualTo(expected);
	}
}

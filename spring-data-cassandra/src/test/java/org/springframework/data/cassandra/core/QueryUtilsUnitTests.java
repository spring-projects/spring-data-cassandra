/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.cassandra.core;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.domain.User;

import com.datastax.driver.core.querybuilder.Insert;

/**
 * Unit tests for {@link QueryUtils}.
 *
 * @author Mark Paluch
 */
public class QueryUtilsUnitTests {

	private final MappingCassandraConverter converter = new MappingCassandraConverter();

	@Test // DATACASS-569
	public void shouldCreateInsertQuery() {

		User user = new User("heisenberg", "Walter", "White");
		Insert insert = QueryUtils.createInsertQuery("user", user, InsertOptions.builder().withIfNotExists().build(),
				converter);

		assertThat(insert.toString())
				.isEqualTo("INSERT INTO user (firstname,id,lastname) VALUES ('Walter','heisenberg','White') IF NOT EXISTS;");
	}
}

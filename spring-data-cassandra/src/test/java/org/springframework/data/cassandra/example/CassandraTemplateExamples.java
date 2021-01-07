/*
 * Copyright 2020-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https:://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.example;

import static org.springframework.data.cassandra.core.query.Criteria.*;
import static org.springframework.data.cassandra.core.query.Query.*;

import org.springframework.data.cassandra.core.CassandraTemplate;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * @author Mark Paluch
 */
// @formatter:off
public class CassandraTemplateExamples {

	private CassandraTemplate template = null;

	void examples() {
		// tag::preparedStatement[]
		template.setUsePreparedStatements(true);

		Actor actorByQuery = template.selectOne(query(where("id").is(42)), Actor.class);

		Actor actorByStatement = template.selectOne(
				SimpleStatement.newInstance("SELECT id, name FROM actor WHERE id = ?", 42),
				Actor.class);
		// end::preparedStatement[]
	}

	static class Actor {

	}

}

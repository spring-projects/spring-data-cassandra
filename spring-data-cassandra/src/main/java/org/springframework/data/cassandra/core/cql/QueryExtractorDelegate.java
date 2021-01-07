/*
 * Copyright 2020-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql;

import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * Utility to extract CQL queries from a {@link Statement}.
 *
 * @author Mark Paluch
 * @since 3.0.5
 */
public class QueryExtractorDelegate {

	/**
	 * Try to extract the {@link SimpleStatement#getQuery() CQL query} from a statement object.
	 *
	 * @param statement the statement object.
	 * @return the CQL query when {@code statement} is not {@code null}.
	 */
	@Nullable
	public static String getCql(@Nullable Object statement) {

		if (statement == null) {
			return null;
		}

		if (statement instanceof CqlProvider) {
			return ((CqlProvider) statement).getCql();
		}

		if (statement instanceof SimpleStatement) {
			return ((SimpleStatement) statement).getQuery();
		}

		if (statement instanceof PreparedStatement) {
			return ((PreparedStatement) statement).getQuery();
		}

		if (statement instanceof BoundStatement) {
			return getCql(((BoundStatement) statement).getPreparedStatement());
		}

		if (statement instanceof BatchStatement) {

			StringBuilder builder = new StringBuilder();

			for (BatchableStatement<?> batchableStatement : ((BatchStatement) statement)) {

				String query = getCql(batchableStatement);
				builder.append(query).append(query.endsWith(";") ? "" : ";");
			}

			return builder.toString();
		}

		return String.format("Unknown: %s", statement);
	}
}

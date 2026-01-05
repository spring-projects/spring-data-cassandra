/*
 * Copyright 2020-present the original author or authors.
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

import org.jspecify.annotations.Nullable;

import org.springframework.lang.Contract;
import org.springframework.util.ObjectUtils;

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
	 * Extract the {@link CqlProvider#getCql() CQL query}.
	 *
	 * @param statement the statement object.
	 * @return the CQL query when {@code statement} is not {@code null}.
	 */
	static String getCql(CqlProvider statement) {
		return statement.getCql();
	}

	/**
	 * Extract the {@link SimpleStatement#getQuery() CQL query}.
	 *
	 * @param statement the statement object.
	 * @return the CQL query when {@code statement} is not {@code null}.
	 */
	static String getCql(SimpleStatement statement) {
		return statement.getQuery();
	}

	/**
	 * Extract the {@link PreparedStatement#getQuery() CQL query}.
	 *
	 * @param statement the statement object.
	 * @return the CQL query when {@code statement} is not {@code null}.
	 */
	static String getCql(PreparedStatement statement) {
		return statement.getQuery();
	}

	/**
	 * Extract the {@link BoundStatement CQL query}.
	 *
	 * @param statement the statement object.
	 * @return the CQL query when {@code statement} is not {@code null}.
	 */
	static String getCql(BoundStatement statement) {
		return getCql(statement.getPreparedStatement());
	}

	/**
	 * Extract the {@link BatchStatement CQL query}.
	 *
	 * @param statement the statement object.
	 * @return the CQL query when {@code statement} is not {@code null}.
	 */
	static String getCql(BatchStatement statement) {

		StringBuilder builder = new StringBuilder();

		for (BatchableStatement<?> batchableStatement : ((BatchStatement) statement)) {

			String query = getCql(batchableStatement);
			builder.append(query);

			if (!ObjectUtils.isEmpty(query)) {
				builder.append(query.endsWith(";") ? "" : ";");
			}
		}

		return builder.toString();
	}

	/**
	 * Try to extract the {@link SimpleStatement#getQuery() CQL query} from a statement object.
	 *
	 * @param statement the statement object.
	 * @return the CQL query when {@code statement} is not {@code null}.
	 */

	@Contract("null -> null; !null -> !null")
	public static @Nullable String getCql(@Nullable Object statement) {

		if (statement == null) {
			return null;
		}

		if (statement instanceof CqlProvider cp) {
			return getCql(cp);
		}

		if (statement instanceof SimpleStatement st) {
			return getCql(st);
		}

		if (statement instanceof PreparedStatement pst) {
			return getCql(pst);
		}

		if (statement instanceof BoundStatement bst) {
			return getCql(bst);
		}

		if (statement instanceof BatchStatement bst) {
			return getCql(bst);
		}

		return "Unknown: " + statement;
	}

}

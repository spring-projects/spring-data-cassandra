/*
 * Copyright 2013-2022 the original author or authors.
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
package org.springframework.data.cassandra.observability;

import io.micrometer.common.Tags;

import java.util.Optional;
import java.util.StringJoiner;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * Default {@link CqlSessionTagsProvider} implementation.
 *
 * @author Greg Turnquist
 * @since 4.0.0
 */
public class DefaultCassandraTagsProvider implements CqlSessionTagsProvider {

	@Override
	public Tags getLowCardinalityTags(CqlSessionContext context) {

		Tags tags = Tags.of( //
				CassandraObservation.LowCardinalityTags.SESSION_NAME
						.of(Optional.ofNullable(context.getDelegateSession().getName()).orElse("unknown")),
				CassandraObservation.LowCardinalityTags.KEYSPACE_NAME.of(
						Optional.ofNullable(context.getStatement().getKeyspace()).map(CqlIdentifier::asInternal).orElse("unknown")),
				CassandraObservation.LowCardinalityTags.METHOD_NAME.of(context.getMethodName()));

		if (context.getStatement().getNode() != null) {
			tags = tags.and(CassandraObservation.LowCardinalityTags.URL
					.of(context.getStatement().getNode().getEndPoint().resolve().toString()));
		}

		return tags;
	}

	@Override
	public Tags getHighCardinalityTags(CqlSessionContext context) {
		return Tags.of(CassandraObservation.HighCardinalityTags.CQL_TAG.of(getCql(context.getStatement())));
	}

	/**
	 * Extract the CQL query from the delegate {@link Statement}.
	 *
	 * @return string-based CQL of the delegate
	 * @param statement
	 */
	private static String getCql(Statement<?> statement) {

		String query = "";

		if (statement instanceof SimpleStatement) {
			query = getQuery(statement);
		}

		if (statement instanceof BatchStatement) {

			StringJoiner joiner = new StringJoiner(";");

			for (BatchableStatement<?> bs : (BatchStatement) statement) {
				joiner.add(getQuery(bs));
			}

			query = joiner.toString();
		}

		return query;
	}

	/**
	 * Extract the query from a {@link Statement}.
	 *
	 * @param statement
	 * @return query
	 */
	private static String getQuery(Statement<?> statement) {

		if (statement instanceof SimpleStatement) {
			return ((SimpleStatement) statement).getQuery();
		}

		if (statement instanceof BoundStatement) {
			return ((BoundStatement) statement).getPreparedStatement().getQuery();
		}

		return "";
	}
}

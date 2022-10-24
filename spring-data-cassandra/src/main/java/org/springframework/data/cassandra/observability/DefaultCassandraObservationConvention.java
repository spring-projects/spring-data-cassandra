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

import java.util.StringJoiner;

import org.springframework.data.cassandra.observability.CassandraObservation.HighCardinalityKeyNames;
import org.springframework.data.cassandra.observability.CassandraObservation.LowCardinalityKeyNames;
import org.springframework.util.StringUtils;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

import io.micrometer.common.KeyValues;

/**
 * Default {@link CassandraObservationConvention} implementation.
 *
 * @author Greg Turnquist
 * @author Mark Paluch
 * @since 4.0
 */
class DefaultCassandraObservationConvention implements CassandraObservationConvention {

	@Override
	public KeyValues getLowCardinalityKeyValues(CassandraObservationContext context) {

		KeyValues keyValues = KeyValues.of(LowCardinalityKeyNames.SESSION_NAME.withValue(context.getSessionName()),
				LowCardinalityKeyNames.KEYSPACE_NAME.withValue(context.getKeyspaceName()),
				LowCardinalityKeyNames.METHOD_NAME.withValue(context.getMethodName()));

		if (context.getStatement().getNode() != null) {
			keyValues = keyValues.and(
					LowCardinalityKeyNames.URL.withValue(context.getStatement().getNode().getEndPoint().resolve().toString()));
		}

		return keyValues;
	}

	@Override
	public KeyValues getHighCardinalityKeyValues(CassandraObservationContext context) {
		return KeyValues.of(HighCardinalityKeyNames.CQL_TAG.withValue(getCql(context.getStatement())));
	}

	@Override
	public String getContextualName(CassandraObservationContext context) {
		return (context.isPrepare() ? "PREPARE: " : "") + getSpanName(getCql(context.getStatement()), "");
	}

	/**
	 * Extract the CQL query from the delegate {@link Statement}.
	 *
	 * @return string-based CQL of the delegate
	 * @param statement
	 */
	private static String getCql(Statement<?> statement) {

		String query = "";

		if (statement instanceof SimpleStatement || statement instanceof BoundStatement) {
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

	/**
	 * Tries to parse the CQL query or provides the default name.
	 *
	 * @param defaultName if there's no query
	 * @return span name
	 */
	public String getSpanName(String cql, String defaultName) {

		if (StringUtils.hasText(cql) && cql.indexOf(' ') > -1) {
			return cql.substring(0, cql.indexOf(' '));
		}

		return defaultName;
	}
}

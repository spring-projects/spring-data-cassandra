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

import io.micrometer.common.KeyValues;

import java.util.Optional;
import java.util.StringJoiner;

import org.springframework.data.cassandra.observability.CassandraObservation.HighCardinalityKeyNames;
import org.springframework.data.cassandra.observability.CassandraObservation.LowCardinalityKeyNames;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.*;

/**
 * Default {@link CqlSessionObservationConvention} implementation.
 *
 * @author Greg Turnquist
 * @since 4.0.0
 */
public class DefaultCassandraObservationContention implements CqlSessionObservationConvention {

	@Override
	public KeyValues getLowCardinalityKeyValues(CqlSessionContext context) {

		KeyValues keyValues = KeyValues.of(
				LowCardinalityKeyNames.SESSION_NAME
						.withValue(Optional.ofNullable(context.getDelegateSession().getName()).orElse("unknown")),
				LowCardinalityKeyNames.KEYSPACE_NAME.withValue(
						Optional.ofNullable(context.getStatement().getKeyspace()).map(CqlIdentifier::asInternal).orElse("unknown")),
				LowCardinalityKeyNames.METHOD_NAME.withValue(context.getMethodName()));

		if (context.getStatement().getNode() != null) {
			keyValues = keyValues.and(
					LowCardinalityKeyNames.URL.withValue(context.getStatement().getNode().getEndPoint().resolve().toString()));
		}

		return keyValues;
	}

	@Override
	public KeyValues getHighCardinalityKeyValues(CqlSessionContext context) {
		return KeyValues.of(HighCardinalityKeyNames.CQL_TAG.withValue(getCql(context.getStatement())));
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

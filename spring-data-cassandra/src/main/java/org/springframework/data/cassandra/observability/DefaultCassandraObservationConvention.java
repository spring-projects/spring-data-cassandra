/*
 * Copyright 2022-present the original author or authors.
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

import java.net.InetSocketAddress;
import java.util.StringJoiner;

import org.jspecify.annotations.Nullable;

import org.springframework.data.cassandra.observability.CassandraObservation.HighCardinalityKeyNames;
import org.springframework.data.cassandra.observability.CassandraObservation.LowCardinalityKeyNames;
import org.springframework.util.StringUtils;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;

/**
 * Default {@link CassandraObservationConvention} implementation.
 *
 * @author Greg Turnquist
 * @author Mark Paluch
 * @since 4.0
 */
public class DefaultCassandraObservationConvention implements CassandraObservationConvention {

	public static final CassandraObservationConvention INSTANCE = new DefaultCassandraObservationConvention();

	@Override
	public KeyValues getLowCardinalityKeyValues(CassandraObservationContext context) {

		String dbOperation = context.isPrepare() ? "PREPARE" : getOperationName(getCql(context.getStatement()), "");

		KeyValues keyValues = KeyValues.of(LowCardinalityKeyNames.DATABASE_SYSTEM.withValue("cassandra"),
				LowCardinalityKeyNames.KEYSPACE_NAME.withValue(context.getKeyspaceName()),
				LowCardinalityKeyNames.SESSION_NAME.withValue(context.getSessionName()),
				LowCardinalityKeyNames.METHOD_NAME.withValue(context.getMethodName()),
				LowCardinalityKeyNames.DB_OPERATION.withValue(dbOperation));

		Node node = context.getNode();

		if (node == null) {
			node = context.getStatement().getNode();
		}

		if (node != null) {

			EndPoint endPoint = node.getEndPoint();

			keyValues = keyValues.and(LowCardinalityKeyNames.COORDINATOR.withValue("" + node.getHostId()),
					LowCardinalityKeyNames.COORDINATOR_DC.withValue("" + node.getDatacenter()));

			keyValues.and(LowCardinalityKeyNames.NET_PEER_NAME.withValue(endPoint.toString()));
			InetSocketAddress socketAddress = tryGetSocketAddress(endPoint);

			if (socketAddress != null) {

				keyValues = keyValues.and(LowCardinalityKeyNames.NET_TRANSPORT.withValue("IP.TCP"),
						LowCardinalityKeyNames.NET_SOCK_PEER_ADDR.withValue(socketAddress.getHostString()),
						LowCardinalityKeyNames.NET_SOCK_PEER_PORT.withValue("" + socketAddress.getPort()));
			}
		}

		return keyValues;
	}

	@Override
	public KeyValues getHighCardinalityKeyValues(CassandraObservationContext context) {

		Statement<?> statement = context.getStatement();

		KeyValues keyValues = KeyValues.of(HighCardinalityKeyNames.DB_STATEMENT.withValue(getCql(statement)),
				HighCardinalityKeyNames.PAGE_SIZE.withValue("" + statement.getPageSize()));

		Boolean idempotent = statement.isIdempotent();
		if (idempotent != null) {
			keyValues = keyValues
					.and(HighCardinalityKeyNames.IDEMPOTENCE.withValue(idempotent ? "idempotent" : "non-idempotent"));
		}

		ConsistencyLevel consistencyLevel = statement.getConsistencyLevel();
		if (consistencyLevel != null) {
			keyValues = keyValues.and(HighCardinalityKeyNames.CONSISTENCY_LEVEL.withValue("" + consistencyLevel.name()));
		}

		return keyValues;
	}

	protected @Nullable InetSocketAddress tryGetSocketAddress(EndPoint endPoint) {

		try {
			if (endPoint.resolve() instanceof InetSocketAddress inet) {
				return inet;
			}

		} catch (RuntimeException e) {}

		return null;
	}

	@Override
	public String getContextualName(CassandraObservationContext context) {
		return (context.isPrepare() ? "PREPARE: " : "") + getOperationName(getCql(context.getStatement()), "");
	}

	/**
	 * Tries to parse the CQL query or provides the default name.
	 *
	 * @param defaultName if there's no query
	 * @return span name
	 */
	public String getOperationName(String cql, String defaultName) {

		if (StringUtils.hasText(cql) && cql.indexOf(' ') > -1) {
			return cql.substring(0, cql.indexOf(' '));
		}

		return defaultName;
	}

	/**
	 * Extract the CQL query from the delegate {@link Statement}.
	 *
	 * @return string-based CQL of the delegate
	 * @param statement
	 */
	protected static String getCql(Statement<?> statement) {

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
	protected static String getQuery(Statement<?> statement) {

		if (statement instanceof SimpleStatement) {
			return ((SimpleStatement) statement).getQuery();
		}

		if (statement instanceof BoundStatement) {
			return ((BoundStatement) statement).getPreparedStatement().getQuery();
		}

		return "";
	}
}

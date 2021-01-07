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
package org.springframework.data.cassandra.core;

import java.util.Map;

import org.slf4j.Logger;

import org.springframework.data.cassandra.core.cql.QueryExtractorDelegate;
import org.springframework.util.StringUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Support class for Cassandra Template API implementation classes that want to make use of prepared statements.
 *
 * @author Mark Paluch
 * @since 3.2
 */
class PreparedStatementDelegate {

	/**
	 * Bind values held in {@link SimpleStatement} to the {@link PreparedStatement}.
	 *
	 * @param statement
	 * @param ps
	 * @return the bound statement.
	 */
	static BoundStatement bind(SimpleStatement statement, PreparedStatement ps) {

		BoundStatementBuilder boundStatementBuilder = ps.boundStatementBuilder(statement.getPositionalValues().toArray());
		Map<CqlIdentifier, Object> namedValues = statement.getNamedValues();

		ColumnDefinitions variableDefinitions = ps.getVariableDefinitions();
		for (Map.Entry<CqlIdentifier, Object> entry : namedValues.entrySet()) {

			if (entry.getValue() == null) {
				boundStatementBuilder = boundStatementBuilder.setToNull(entry.getKey());
			} else {
				DataType type = variableDefinitions.get(entry.getKey()).getType();
				boundStatementBuilder = boundStatementBuilder.set(entry.getKey(), entry.getValue(),
						boundStatementBuilder.codecRegistry().codecFor(type));
			}
		}

		return ps.bind(statement.getPositionalValues().toArray());
	}

	/**
	 * Ensure the given {@link Statement} is a {@link SimpleStatement}. Throw a {@link IllegalArgumentException}
	 * otherwise.
	 *
	 * @param statement
	 * @return the {@link SimpleStatement}.
	 */
	static SimpleStatement getStatementForPrepare(Statement<?> statement) {

		if (statement instanceof SimpleStatement) {
			return (SimpleStatement) statement;
		}

		throw new IllegalArgumentException(getMessage(statement));
	}

	/**
	 * Check whether to use prepared statements. When {@code usePreparedStatements} is {@literal true}, then verifying
	 * additionally that the given {@link Statement} is a {@link SimpleStatement}, otherwise log the mismatch and fallback
	 * to non-prepared usage.
	 *
	 * @param usePreparedStatements
	 * @param statement
	 * @param logger
	 * @return
	 */
	static boolean canPrepare(boolean usePreparedStatements, Statement<?> statement, Logger logger) {

		if (usePreparedStatements) {

			if (statement instanceof SimpleStatement) {
				return true;
			}

			logger.warn(getMessage(statement));
		}

		return false;
	}

	private static String getMessage(Statement<?> statement) {

		String cql = QueryExtractorDelegate.getCql(statement);

		if (StringUtils.hasText(cql)) {
			return String.format("Cannot prepare statement %s (%s). Statement must be a SimpleStatement.", cql, statement);
		}

		return String.format("Cannot prepare statement %s. Statement must be a SimpleStatement.", statement);
	}

}

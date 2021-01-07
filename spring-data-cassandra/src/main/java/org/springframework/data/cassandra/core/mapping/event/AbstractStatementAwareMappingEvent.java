/*
 * Copyright 2018-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping.event;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * Event encapsulating Cassandra CQL statement.
 *
 * @author Lukasz Antoniak
 * @author Mark Paluch
 * @since 2.1
 */
public abstract class AbstractStatementAwareMappingEvent<T> extends CassandraMappingEvent<T> {

	private final Statement<?> statement;

	/**
	 * Creates new {@link AbstractStatementAwareMappingEvent}.
	 *
	 * @param source must not be {@literal null}.
	 * @param statement must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 */
	public AbstractStatementAwareMappingEvent(T source, Statement<?> statement, CqlIdentifier tableName) {

		super(source, tableName);
		this.statement = statement;
	}

	/**
	 * @return CQL statement that is going to be executed.
	 */
	public Statement getStatement() {
		return statement;
	}
}

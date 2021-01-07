/*
 * Copyright 2016-2021 the original author or authors.
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

import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.Statement;
import org.reactivestreams.Publisher;

import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.ReactiveSession;

/**
 * Generic callback interface for code that operates on a CQL {@link Statement}. Allows to execute any number of
 * operations on a single {@link Statement}, for example a single {@link ReactiveSession#execute(Statement)}.
 * <p>
 * Used internally by {@link ReactiveCqlTemplate}, but also useful for application code.
 *
 * @param <T>
 * @author Mark Paluch
 * @since 2.0
 */
@FunctionalInterface
public interface ReactiveStatementCallback<T> {

	/**
	 * Gets called by {@link ReactiveCqlTemplate#execute(String)} with an active Cassandra session. Does not need to care
	 * about closing the the session: this will all be handled by Spring's {@link ReactiveCqlTemplate}.
	 * <p>
	 * Allows for returning a result object created within the callback, i.e. a domain object or a collection of domain
	 * objects. Note that there's special support for single step actions: see
	 * {@link ReactiveCqlTemplate#queryForObject(String, Class, Object...)} etc. A thrown RuntimeException is treated as
	 * application exception, it gets propagated to the caller of the template.
	 *
	 * @param session active Cassandra session.
	 * @param stmt CQL Statement.
	 * @return a result object publisher.
	 * @throws DriverException if thrown by a session method, to be auto-converted to a {@link DataAccessException}
	 * @throws DataAccessException in case of custom exceptions
	 * @see ReactiveCqlTemplate#query(String, ReactiveResultSetExtractor)
	 * @see ReactiveCqlTemplate#query(Statement, ReactiveResultSetExtractor)
	 */
	Publisher<T> doInStatement(ReactiveSession session, Statement<?> stmt) throws DriverException, DataAccessException;
}

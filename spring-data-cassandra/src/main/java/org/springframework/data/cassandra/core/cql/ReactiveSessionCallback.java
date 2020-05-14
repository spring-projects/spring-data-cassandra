/*
 * Copyright 2016-2020 the original author or authors.
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
 * Generic callback interface for code that operates on a CQL {@link ReactiveSession}. Allows to execute any number of
 * operations on a single {@link ReactiveSession}, using any type and number of Statements.
 * <p>
 * This is particularly useful for delegating to existing data access code that expects a {@link ReactiveSession} to
 * work on and throws {@link DriverException}. For newly written code, it is strongly recommended to use
 * {@link CqlTemplate}'s more specific operations, for example a query or update variant.
 *
 * @param <T>
 * @author Mark Paluch
 * @since 2.0
 * @see ReactiveCqlTemplate#execute(ReactiveSessionCallback)
 */
@FunctionalInterface
public interface ReactiveSessionCallback<T> {

	/**
	 * Gets called by {@link ReactiveCqlTemplate#execute(ReactiveSessionCallback)} with an active Cassandra session. Does
	 * not need to care about activating or closing the {@link ReactiveSession}.
	 * <p>
	 * Allows for returning a result object created within the callback, i.e. a domain object or a collection of domain
	 * objects. Note that there's special support for single step actions: see
	 * {@link ReactiveCqlTemplate#queryForObject(Statement, Class)} etc. A thrown {@link RuntimeException} is treated as
	 * application exception: it gets propagated to the caller of the template.
	 *
	 * @param session active Cassandra session.
	 * @return a result object publisher.
	 * @throws DriverException if thrown by a session method, to be auto-converted to a {@link DataAccessException}.
	 * @throws DataAccessException in case of custom exceptions.
	 * @see ReactiveCqlTemplate#execute(ReactivePreparedStatementCreator, ReactivePreparedStatementCallback)
	 */
	Publisher<T> doInSession(ReactiveSession session) throws DriverException, DataAccessException;
}

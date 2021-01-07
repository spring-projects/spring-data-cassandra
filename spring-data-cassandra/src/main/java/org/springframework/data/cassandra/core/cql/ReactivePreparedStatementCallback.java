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
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.reactivestreams.Publisher;

import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.ReactiveSession;

/**
 * Generic callback interface for code that operates on a {@link PreparedStatement}. Allows to execute any number of
 * operations on a single {@link PreparedStatement}, for example a single {@link ReactiveSession#execute(Statement).
 * <p>
 * Used internally by {@link ReactiveCqlTemplate}, but also useful for application code. Note that the passed-in
 * {@link PreparedStatement} can have been created by the framework or by a custom
 * {@link ReactivePreparedStatementCreator}. However, the latter is hardly ever necessary, as most custom callback
 * actions will perform updates in which case a standard {@link PreparedStatement is fine. Custom actions will always
 * set parameter values themselves, so that {@link ReactivePreparedStatementCreator} capability is not needed either.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see ReactiveCqlTemplate#execute(ReactivePreparedStatementCreator, ReactivePreparedStatementCallback)
 * @see ReactiveCqlTemplate#execute(String, ReactivePreparedStatementCallback)
 */
@FunctionalInterface
public interface ReactivePreparedStatementCallback<T> {

	/**
	 * Gets called by {@link ReactiveCqlTemplate#execute(String, ReactivePreparedStatementCallback)} with an active CQL
	 * session and {@link PreparedStatement}. Does not need to care about closing the session: this will all be handled by
	 * Spring's {@link ReactiveCqlTemplate}.
	 * <p>
	 * Allows for returning a result object created within the callback, i.e. a domain object or a collection of domain
	 * objects. Note that there's special support for single step actions: see
	 * {@link ReactiveCqlTemplate#queryForObject(String, Class, Object...)} etc. A thrown RuntimeException is treated as
	 * application exception, it gets propagated to the caller of the template.
	 *
	 * @param session active Cassandra session, must not be {@literal null}.
	 * @param ps the {@link PreparedStatement}, must not be {@literal null}.
	 * @return a result object publisher.
	 * @throws DriverException if thrown by a session method, to be auto-converted to a {@link DataAccessException}.
	 * @throws DataAccessException in case of custom exceptions.
	 * @see ReactiveCqlTemplate#queryForObject(String, Class, Object...)
	 * @see ReactiveCqlTemplate#queryForFlux(String, Object...)
	 */
	Publisher<T> doInPreparedStatement(ReactiveSession session, PreparedStatement ps)
			throws DriverException, DataAccessException;
}

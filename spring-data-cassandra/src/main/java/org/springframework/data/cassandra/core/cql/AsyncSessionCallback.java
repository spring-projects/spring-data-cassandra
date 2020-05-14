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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;

import org.springframework.dao.DataAccessException;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * Generic callback interface for code that operates asynchronously on a Cassandra {@link CqlSession}. Allows to execute
 * any number of operations on a single session, using any type and number of statements.
 * <p>
 * This is particularly useful for delegating to existing data access code that expects a {@link CqlSession} to work on
 * and throws {@link DriverException}. For newly written code, it is strongly recommended to use {@link CqlTemplate}'s
 * more specific operations, for example a {@code query} or {@code update} variant.
 *
 * @author David Webb
 * @author Mark Paluch
 * @see AsyncCqlTemplate#execute(AsyncSessionCallback)
 * @see AsyncCqlTemplate#query
 * @see AsyncCqlTemplate#execute(String)
 */
@FunctionalInterface
public interface AsyncSessionCallback<T> {

	/**
	 * Gets called by {@link CqlTemplate#execute} with an active Cassandra {@link CqlSession}. Does not need to care about
	 * activating or closing the {@link CqlSession}.
	 * <p>
	 * Allows for returning a result object created within the callback, i.e. a domain object or a collection of domain
	 * objects. Note that there's special support for single step actions: see {@link CqlTemplate#queryForObject} etc. A
	 * thrown {@link RuntimeException} is treated as application exception: it gets propagated to the caller of the
	 * template.
	 *
	 * @param session active Cassandra Session, must not be {@literal null}.
	 * @return a result object, or {@code ListenableFuture<Void>} if none.
	 * @throws DriverException if thrown by a Session method, to be auto-converted to a {@link DataAccessException}.
	 * @throws DataAccessException in case of custom exceptions.
	 * @see AsyncCqlTemplate#queryForObject(String, Class)
	 * @see AsyncCqlTemplate#queryForResultSet(String)
	 */
	ListenableFuture<T> doInSession(CqlSession session) throws DriverException, DataAccessException;
}

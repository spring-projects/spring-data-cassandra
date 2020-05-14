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
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

import org.springframework.util.concurrent.ListenableFuture;

/**
 * One of the two central callback interfaces used by the {@link AsyncCqlTemplate} class. This interface prepares a CQL
 * statement returning a {@link org.springframework.util.concurrent.ListenableFuture} given a {@link CqlSession},
 * provided by the {@link CqlTemplate} class.
 * <p>
 * Implementations may either create new prepared statements or reuse cached instances. Implementations do not need to
 * concern themselves with {@link DriverException}s that may be thrown from operations they attempt. The
 * {@link AsyncCqlTemplate} class will catch and handle {@link DriverException}s appropriately.
 * <p>
 * A {@link AsyncPreparedStatementCreator} should also implement the {@link CqlProvider} interface if it is able to
 * provide the CQL it uses for {@link PreparedStatement} creation. This allows for better contextual information in case
 * of exceptions.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see AsyncCqlTemplate#execute(AsyncPreparedStatementCreator, PreparedStatementCallback)
 */
@FunctionalInterface
public interface AsyncPreparedStatementCreator {

	/**
	 * Create a statement in this session. Allows implementations to use {@link PreparedStatement}s. The
	 * {@link CqlTemplate} will attempt to cache the {@link PreparedStatement}s for future use without the overhead of
	 * re-preparing on the entire cluster.
	 *
	 * @param session Session to use to create statement, must not be {@literal null}.
	 * @return a prepared statement.
	 * @throws DriverException there is no need to catch DriverException that may be thrown in the implementation of this
	 *           method. The {@link AsyncCqlTemplate} class will handle them.
	 */
	ListenableFuture<PreparedStatement> createPreparedStatement(CqlSession session) throws DriverException;
}

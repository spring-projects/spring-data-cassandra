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

import reactor.core.publisher.Mono;

import org.springframework.data.cassandra.ReactiveSession;

import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

/**
 * One of the two central callback interfaces used by the {@link ReactiveCqlTemplate} class. This interface creates a
 * {@link PreparedStatement} given a {@link ReactiveSession}, provided by the {@link ReactiveCqlTemplate} class.
 * <p>
 * Implementations may either create new prepared statements or reuse cached instances. Implementations do not need to
 * concern themselves with {@link DriverException}s that may be thrown from operations they attempt. The
 * {@link ReactiveCqlTemplate} class will catch and handle {@link DriverException}s appropriately.
 * <p>
 * Classes implementing this interface should also implement the {@link CqlProvider} interface if it is able to provide
 * the CQL it uses for {@link PreparedStatement} creation. This allows for better contextual information in case of
 * exceptions.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see ReactiveCqlTemplate#execute(ReactivePreparedStatementCreator, ReactivePreparedStatementCallback)
 * @see CqlProvider
 */
@FunctionalInterface
public interface ReactivePreparedStatementCreator {

	/**
	 * Create a statement in this session. Allows implementations to use {@link PreparedStatement}s. The
	 * {@link ReactiveCqlTemplate} will attempt to cache the {@link PreparedStatement}s for future use without the
	 * overhead of re-preparing on the entire cluster.
	 *
	 * @param session Session to use to create statement, must not be {@literal null}.
	 * @return a prepared statement.
	 * @throws DriverException there is no need to catch DriverException that may be thrown in the implementation of this
	 *           method. The {@link ReactiveCqlTemplate} class will handle them.
	 */
	Mono<PreparedStatement> createPreparedStatement(ReactiveSession session) throws DriverException;
}

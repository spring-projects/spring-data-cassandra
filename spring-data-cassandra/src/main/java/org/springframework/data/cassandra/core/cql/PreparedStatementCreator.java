/*
 * Copyright 2013-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.cql;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;

/**
 * One of the two central callback interfaces used by the {@link CqlTemplate} class. This interface creates a
 * {@link PreparedStatement} given a session, provided by the {@link CqlTemplate} class. Implementations are responsible
 * for providing CQL and any necessary parameters.
 * <p>
 * Implementations <i>do not</i> need to concern themselves with {@link DriverException}s that may be thrown from
 * operations they attempt. The {@link CqlTemplate} class will catch and handle {@link DriverException}s appropriately.
 *
 * @author David Webb
 * @author Mark Paluch
 * @see CqlTemplate#execute(PreparedStatementCreator, PreparedStatementCallback)
 * @see CqlTemplate#query(PreparedStatementCreator, RowCallbackHandler)
 */
@FunctionalInterface
public interface PreparedStatementCreator {

	/**
	 * Create a statement in this session. Allows implementations to use {@link PreparedStatement}.
	 *
	 * @param session {@link Session} to use to create statement.
	 * @return a prepared statement.
	 * @throws DriverException there is no need to catch {@link DriverException} that may be thrown in the implementation
	 *           of this method. The {@link CqlTemplate} class will handle them.
	 */
	PreparedStatement createPreparedStatement(Session session) throws DriverException;
}

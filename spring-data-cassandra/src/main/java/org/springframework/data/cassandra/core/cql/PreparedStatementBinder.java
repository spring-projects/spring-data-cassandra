/*
 * Copyright 2013-2021 the original author or authors.
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
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

/**
 * General callback interface used by the {@link CqlTemplate} and {@link ReactiveCqlTemplate} classes.
 * <p>
 * This interface binds values on a {@link PreparedStatement} provided by the {@link CqlTemplate} class, for each of a
 * number of updates in a batch using the same CQL. Implementations are responsible for setting any necessary
 * parameters. CQL with placeholders will already have been supplied.
 * <p>
 * It's easier to use this interface than {@link PreparedStatementCreator}: The {@link CqlTemplate} will create the
 * {@link PreparedStatement}, with the callback only being responsible for setting parameter values.
 * <p>
 * Implementations <i>do not</i> need to concern themselves with {@link DriverException}s that may be thrown from
 * operations they attempt. The {@link CqlTemplate} class will catch and handle {@link DriverException} appropriately.
 *
 * @author David Webb
 * @author Mark Paluch
 * @see CqlTemplate#query(String, PreparedStatementBinder, ResultSetExtractor)
 * @see AsyncCqlTemplate#query(String, PreparedStatementBinder, AsyncResultSetExtractor)
 * @see ReactiveCqlTemplate#query(String, PreparedStatementBinder, ReactiveResultSetExtractor)
 */
@FunctionalInterface
public interface PreparedStatementBinder {

	/**
	 * Bind parameter values on the given {@link PreparedStatement}.
	 *
	 * @param ps the PreparedStatement to invoke setter methods on.
	 * @throws DriverException if a {@link DriverException} is encountered (i.e. there is no need to catch
	 *           {@link DriverException})
	 */
	BoundStatement bindValues(PreparedStatement ps) throws DriverException;
}

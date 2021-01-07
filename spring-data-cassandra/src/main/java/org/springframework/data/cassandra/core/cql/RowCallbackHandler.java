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
import com.datastax.oss.driver.api.core.cql.Row;

/**
 * An interface used by {@link CqlTemplate} for processing rows of a
 * {@link com.datastax.oss.driver.api.core.cql.ResultSet} on a per-row basis. Implementations of this interface perform
 * the actual work of processing each row but don't need to worry about exception handling. {@link DriverException}s
 * will be caught and handled by the calling {@link CqlTemplate}.
 * <p>
 * In contrast to a {@link ResultSetExtractor}, a {@link RowCallbackHandler} object is typically stateful: It keeps the
 * result state within the object, to be available for later inspection.
 * <p>
 * Consider using a {@link RowMapper} instead if you need to map exactly one result object per row, assembling them into
 * a List.
 *
 * @author Mark Paluch
 * @see CqlTemplate
 * @see RowMapper
 * @see ResultSetExtractor
 */
@FunctionalInterface
public interface RowCallbackHandler {

	/**
	 * Implementations must implement this method to process each row of data in the
	 * {@link com.datastax.oss.driver.api.core.cql.ResultSet}. This method is only supposed to extract values of the
	 * current row.
	 * <p>
	 * Exactly what the implementation chooses to do is up to it: A trivial implementation might simply count rows, while
	 * another implementation might build an XML document.
	 *
	 * @param row the {@link Row} to process (pre-initialized for the current row).
	 * @throws DriverException if a {@link DriverException} is encountered getting column values (that is, there's no need
	 *           to catch {@link DriverException}).
	 */
	void processRow(Row row) throws DriverException;
}

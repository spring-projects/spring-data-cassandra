/*
 * Copyright 2013-2020 the original author or authors.
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

import org.springframework.lang.Nullable;

/**
 * An interface used by {@link CqlTemplate} for mapping rows of a {@link com.datastax.driver.core.ResultSet} on a
 * per-row basis. Implementations of this interface perform the actual work of mapping each row to a result object, but
 * don't need to worry about exception handling. {@link DriverException}s will be caught and handled by the calling
 * {@link CqlTemplate}.
 * <p>
 * Typically used either for {@link CqlTemplate}'s query methods or for out parameters of stored procedures.
 * {@link RowMapper} objects are typically stateless and thus reusable; they are an ideal choice for implementing
 * row-mapping logic in a single place.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @see RowCallbackHandler
 * @see ResultSetExtractor
 */
@FunctionalInterface
public interface RowMapper<T> {

	/**
	 * Implementations must implement this method to map each row of data in the
	 * {@link com.datastax.driver.core.ResultSet}.
	 *
	 * @param row the {@link Row} to map, must not be {@literal null}.
	 * @param rowNum the number of the current row.
	 * @return the result object for the current row.
	 * @throws DriverException if a {@link DriverException} is encountered getting column values (that is, there's no need
	 *           to catch {@link DriverException})
	 */
	@Nullable
	T mapRow(Row row, int rowNum) throws DriverException;
}

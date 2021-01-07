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
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;

import org.springframework.dao.DataAccessException;
import org.springframework.lang.Nullable;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * Callback interface used by {@link AsyncCqlTemplate}'s query methods. Implementations of this interface perform the
 * actual work of extracting results from a {@link AsyncResultSet}, but don't need to worry about exception handling.
 * {@link DriverException}s will be caught and handled by the calling {@link AsyncCqlTemplate}.
 * <p>
 * This interface is mainly used within the CQL framework itself. A {@link RowMapper} is usually a simpler choice for
 * {@link AsyncResultSet} processing, mapping one result object per row instead of one result object for the entire
 * {@link AsyncResultSet}.
 * <p>
 * Note: In contrast to a {@link RowCallbackHandler}, a {@link AsyncResultSetExtractor} object is typically stateless
 * and thus reusable, as long as it doesn't access stateful resources or keep result state within the object.
 *
 * @author Mark Paluch
 * @since 3.0
 * @see AsyncCqlTemplate
 * @see RowMapper
 */
@FunctionalInterface
public interface AsyncResultSetExtractor<T> {

	/**
	 * Implementations must implement this method to process the entire {@link AsyncResultSet}.
	 *
	 * @param resultSet {@link AsyncResultSet} to extract data from.
	 * @return an arbitrary result object, or {@literal null} if none (the extractor will typically be stateful in the
	 *         latter case).
	 * @throws DriverException if a {@link DriverException} is encountered getting column values or navigating (that is,
	 *           there's no need to catch {@link DriverException})
	 * @throws DataAccessException in case of custom exceptions
	 */
	@Nullable
	ListenableFuture<T> extractData(AsyncResultSet resultSet) throws DriverException, DataAccessException;
}

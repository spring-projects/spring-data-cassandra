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
import org.reactivestreams.Publisher;

import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.ReactiveResultSet;

/**
 * Callback interface used by {@link ReactiveCqlTemplate}'s query methods. Implementations of this interface perform the
 * actual work of extracting results from a {@link ReactiveResultSet}, but don't need to worry about exception handling.
 * {@link DriverException}s will be caught and handled by the calling {@link ReactiveCqlTemplate}.
 * <p>
 * This interface is mainly used within the CQL framework itself. A {@link RowMapper} is usually a simpler choice for
 * {@link ReactiveResultSet} processing, mapping one result object per row instead of one result object for the entire
 * {@link ReactiveResultSet}.
 * <p>
 * Note: {@link ReactiveResultSetExtractor} object is typically stateless and thus reusable, as long as it doesn't
 * access stateful resources or keep result state within the object.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see ReactiveCqlTemplate
 * @see RowCallbackHandler
 * @see RowMapper
 */
@FunctionalInterface
public interface ReactiveResultSetExtractor<T> {

	/**
	 * Implementations must implement this method to process the entire {@link ReactiveResultSet}.
	 *
	 * @param resultSet {@link ReactiveResultSet} to extract data from, must not be {@literal null}.
	 * @return an arbitrary result object {@link Publisher}.
	 * @throws DriverException if a {@link DriverException} is encountered getting column values or navigating (that is,
	 *           there's no need to catch {@link DriverException}).
	 * @throws DataAccessException in case of custom exceptions.
	 */
	Publisher<T> extractData(ReactiveResultSet resultSet) throws DriverException, DataAccessException;
}

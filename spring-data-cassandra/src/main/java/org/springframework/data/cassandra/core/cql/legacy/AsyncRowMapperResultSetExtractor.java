/*
 * Copyright 2016-2025 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.legacy;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.core.cql.AsyncCqlTemplate;
import org.springframework.data.cassandra.core.cql.ResultSetExtractor;
import org.springframework.data.cassandra.core.cql.RowMapper;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.ListenableFuture;

import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;

/**
 * Adapter implementation of the {@link ResultSetExtractor} interface that delegates to a {@link RowMapper} which is
 * supposed to create an object for each row. Each object is added to the results List of this
 * {@link ResultSetExtractor}.
 * <p>
 * Useful for the typical case of one object per row in the database table. The number of entries in the results will
 * match the number of rows.
 * <p>
 * Note that a {@link RowMapper} object is typically stateless and thus reusable.
 *
 * @author Mark Paluch
 * @since 4.0
 * @see RowMapper
 * @see AsyncCqlTemplate
 * @deprecated since 4.0, use the {@link java.util.concurrent.CompletableFuture}-based variant.
 */
@Deprecated(since = "4.0", forRemoval = true)
@SuppressWarnings("removal")
public class AsyncRowMapperResultSetExtractor<T> implements AsyncResultSetExtractor<List<T>> {

	private final RowMapper<T> rowMapper;

	/**
	 * Create a new {@link AsyncRowMapperResultSetExtractor}.
	 *
	 * @param rowMapper the {@link RowMapper} which creates an object for each row, must not be {@literal null}.
	 */
	public AsyncRowMapperResultSetExtractor(RowMapper<T> rowMapper) {

		Assert.notNull(rowMapper, "RowMapper is must not be null");

		this.rowMapper = rowMapper;
	}

	@Override
	public ListenableFuture<List<T>> extractData(AsyncResultSet resultSet) throws DriverException, DataAccessException {
		return AsyncResultStream.from(resultSet).map(rowMapper).collect(Collectors.toList());
	}
}

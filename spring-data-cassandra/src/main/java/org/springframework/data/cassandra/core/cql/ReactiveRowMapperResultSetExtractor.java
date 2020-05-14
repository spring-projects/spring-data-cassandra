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

import com.datastax.oss.driver.api.core.DriverException;
import org.reactivestreams.Publisher;

import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.util.Assert;

/**
 * Adapter implementation of the {@link ReactiveResultSetExtractor} interface that delegates to a {@link RowMapper}
 * which is supposed to create an object for each row. Each object is emitted through the {@link Publisher} of this
 * {@link ReactiveResultSetExtractor}.
 * <p>
 * Useful for the typical case of one object per row in the database table. The number of entries in the results will
 * match the number of rows.
 * <p>
 * Note that a {@link RowMapper} object is typically stateless and thus reusable.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see RowMapper
 * @see ReactiveCqlTemplate
 */
public class ReactiveRowMapperResultSetExtractor<T> implements ReactiveResultSetExtractor<T> {

	private final RowMapper<T> rowMapper;

	/**
	 * Create a new {@link ReactiveRowMapperResultSetExtractor}.
	 *
	 * @param rowMapper the {@link RowMapper} which creates an object for each row, must not be {@literal null}.
	 */
	public ReactiveRowMapperResultSetExtractor(RowMapper<T> rowMapper) {

		Assert.notNull(rowMapper, "RowMapper is must not be null");

		this.rowMapper = rowMapper;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ReactiveResultSetExtractor#extractData(org.springframework.data.cassandra.core.cql.ReactiveResultSet)
	 */
	@Override
	public Publisher<T> extractData(ReactiveResultSet resultSet) throws DriverException, DataAccessException {

		return resultSet.rows().handle((row, sink) -> {

			T value = this.rowMapper.mapRow(row, 0);

			if (value != null) {
				sink.next(value);
			}
		});
	}
}

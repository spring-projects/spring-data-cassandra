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

import java.util.ArrayList;
import java.util.List;

import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import org.springframework.dao.DataAccessException;
import org.springframework.util.Assert;

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
 * @since 2.0
 * @see RowMapper
 * @see CqlTemplate
 */
public class RowMapperResultSetExtractor<T> implements ResultSetExtractor<List<T>> {

	private final int rowsExpected;

	private final RowMapper<T> rowMapper;

	/**
	 * Create a new {@link RowMapperResultSetExtractor}.
	 *
	 * @param rowMapper the {@link RowMapper} which creates an object for each row, must not be {@literal null}.
	 */
	public RowMapperResultSetExtractor(RowMapper<T> rowMapper) {
		this(rowMapper, 0);
	}

	/**
	 * Create a new {@link RowMapperResultSetExtractor}.
	 *
	 * @param rowMapper the {@link RowMapper} which creates an object for each row, must not be {@literal null}.
	 * @param rowsExpected the number of expected rows (just used for optimized collection handling).
	 */
	public RowMapperResultSetExtractor(RowMapper<T> rowMapper, int rowsExpected) {

		Assert.notNull(rowMapper, "RowMapper is must not be null");

		this.rowMapper = rowMapper;
		this.rowsExpected = rowsExpected;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.ResultSetExtractor#extractData(com.datastax.driver.core.ResultSet)
	 */
	@Override
	public List<T> extractData(ResultSet resultSet) throws DriverException, DataAccessException {

		List<T> results = (this.rowsExpected > 0 ? new ArrayList<>(this.rowsExpected) : new ArrayList<>());

		int rowNumber = 0;

		for (Row row : resultSet) {
			results.add(this.rowMapper.mapRow(row, rowNumber++));
		}

		return results;
	}
}

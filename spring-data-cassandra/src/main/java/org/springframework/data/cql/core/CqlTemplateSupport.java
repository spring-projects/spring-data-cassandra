/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.cql.core;

import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;

import org.springframework.data.cql.support.CassandraAccessor;

import com.datastax.driver.core.ResultSet;

/**
 * Support class for CQL template implementation providing utility methods and overridable hook methods.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public abstract class CqlTemplateSupport extends CassandraAccessor {

	/**
	 * Create a new arg-based PreparedStatementSetter using the args passed in. By default, we'll create an
	 * {@link ArgumentPreparedStatementBinder}. This method allows for the creation to be overridden by subclasses.
	 *
	 * @param args object array with arguments
	 * @return the new {@link PreparedStatementBinder} to use
	 */
	protected PreparedStatementBinder newPreparedStatementBinder(Object[] args) {
		return new ArgumentPreparedStatementBinder(args);
	}

	/**
	 * Constructs a new instance of the {@link ResultSetExtractor} initialized with and adapting the given
	 * {@link RowCallbackHandler}.
	 *
	 * @param rowCallbackHandler {@link RowCallbackHandler} to adapt as a {@link ResultSetExtractor}.
	 * @return a {@link ResultSetExtractor} implementation adapting an instance of the {@link RowCallbackHandler}.
	 * @see org.springframework.data.cql.core.AsyncCqlTemplate.RowCallbackHandlerResultSetExtractor
	 * @see org.springframework.data.cql.core.ResultSetExtractor
	 * @see org.springframework.data.cql.core.RowCallbackHandler
	 */
	protected RowCallbackHandlerResultSetExtractor newResultSetExtractor(RowCallbackHandler rowCallbackHandler) {
		return new RowCallbackHandlerResultSetExtractor(rowCallbackHandler);
	}

	/**
	 * Constructs a new instance of the {@link ResultSetExtractor} initialized with and adapting the given
	 * {@link RowMapper}.
	 *
	 * @param rowMapper {@link RowMapper} to adapt as a {@link ResultSetExtractor}.
	 * @return a {@link ResultSetExtractor} implementation adapting an instance of the {@link RowMapper}.
	 * @see org.springframework.data.cql.core.ResultSetExtractor
	 * @see org.springframework.data.cql.core.RowMapper
	 * @see org.springframework.data.cql.core.RowMapperResultSetExtractor
	 */
	protected <T> RowMapperResultSetExtractor<T> newResultSetExtractor(RowMapper<T> rowMapper) {
		return new RowMapperResultSetExtractor<>(rowMapper);
	}

	/**
	 * Constructs a new instance of the {@link ResultSetExtractor} initialized with and adapting the given
	 * {@link RowMapper}.
	 *
	 * @param rowMapper {@link RowMapper} to adapt as a {@link ResultSetExtractor}.
	 * @param rowsExpected number of expected rows in the {@link ResultSet}.
	 * @return a {@link ResultSetExtractor} implementation adapting an instance of the {@link RowMapper}.
	 * @see org.springframework.data.cql.core.ResultSetExtractor
	 * @see org.springframework.data.cql.core.RowMapper
	 * @see org.springframework.data.cql.core.RowMapperResultSetExtractor
	 */
	protected <T> RowMapperResultSetExtractor<T> newResultSetExtractor(RowMapper<T> rowMapper, int rowsExpected) {
		return new RowMapperResultSetExtractor<>(rowMapper, rowsExpected);
	}

	/**
	 * Create a new RowMapper for reading columns as key-value pairs.
	 *
	 * @return the RowMapper to use
	 * @see ColumnMapRowMapper
	 */
	protected RowMapper<Map<String, Object>> newColumnMapRowMapper() {
		return new ColumnMapRowMapper();
	}

	/**
	 * Create a new RowMapper for reading result objects from a single column.
	 *
	 * @param requiredType the type that each result object is expected to match
	 * @return the RowMapper to use
	 * @see SingleColumnRowMapper
	 */
	protected <T> RowMapper<T> newSingleColumnRowMapper(Class<T> requiredType) {
		return SingleColumnRowMapper.newInstance(requiredType);
	}

	/**
	 * Determine CQL from potential provider object.
	 *
	 * @param cqlProvider object that's potentially a {@link CqlProvider}
	 * @return the CQL string, or {@code null}
	 * @see CqlProvider
	 */
	protected static String toCql(Object cqlProvider) {
		return Optional.ofNullable(cqlProvider) //
				.filter(o -> o instanceof CqlProvider) //
				.map(o -> (CqlProvider) o) //
				.map(CqlProvider::getCql) //
				.orElse(null);
	}

	/**
	 * Adapter to enable use of a {@link RowCallbackHandler} inside a {@link ResultSetExtractor}.
	 */
	protected static class RowCallbackHandlerResultSetExtractor implements ResultSetExtractor<Object> {

		private final RowCallbackHandler rowCallbackHandler;

		protected RowCallbackHandlerResultSetExtractor(RowCallbackHandler rowCallbackHandler) {
			this.rowCallbackHandler = rowCallbackHandler;
		}

		/* (non-Javadoc)
		 *
		  @see org.springframework.cassandra.core.ResultSetExtractor#extractData(com.datastax.driver.core.ResultSet)
		 */
		@Override
		public Object extractData(ResultSet resultSet) {

			StreamSupport.stream(resultSet.spliterator(), false).forEach(rowCallbackHandler::processRow);

			return null;
		}
	}

}

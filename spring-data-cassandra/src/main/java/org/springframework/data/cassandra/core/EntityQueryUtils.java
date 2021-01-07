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
package org.springframework.data.cassandra.core;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.data.cassandra.core.cql.RowMapper;
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * Simple utility class for working with the QueryBuilder API using mapped entities.
 * <p>
 * Only intended for internal use.
 *
 * @author Mark Paluch
 * @since 2.0
 */
class EntityQueryUtils {

	private static final Pattern FROM_REGEX = Pattern.compile(" FROM ([\"]?[\\w]*[\\\\.]?[\\w]*[\"]?)[\\s]?",
			Pattern.CASE_INSENSITIVE);

	/**
	 * Read a {@link Slice} of data from the {@link ResultSet} for a {@link Pageable}.
	 *
	 * @param resultSet must not be {@literal null}.
	 * @param mapper must not be {@literal null}.
	 * @param page
	 * @param pageSize
	 * @return the resulting {@link Slice}.
	 */
	static <T> Slice<T> readSlice(ResultSet resultSet, RowMapper<T> mapper, int page, int pageSize) {

		int toRead = resultSet.getAvailableWithoutFetching();

		return readSlice(() -> limit(resultSet.iterator(), toRead), resultSet.getExecutionInfo().getPagingState(), mapper,
				page, pageSize);
	}

	/**
	 * Read a {@link Slice} of data from the {@link ResultSet} for a {@link Pageable}.
	 *
	 * @param resultSet must not be {@literal null}.
	 * @param mapper must not be {@literal null}.
	 * @param page
	 * @param pageSize
	 * @return the resulting {@link Slice}.
	 * @since 3.0
	 */
	static <T> Slice<T> readSlice(AsyncResultSet resultSet, RowMapper<T> mapper, int page, int pageSize) {

		return readSlice(() -> limit(resultSet.currentPage().iterator(), resultSet.remaining()),
				resultSet.getExecutionInfo().getPagingState(), mapper, page, pageSize);
	}

	/**
	 * Read a {@link Slice} of data from the {@link Iterable} of {@link Row}s for a {@link Pageable}.
	 *
	 * @param rows must not be {@literal null}.
	 * @param pagingState
	 * @param mapper must not be {@literal null}.
	 * @param page
	 * @param pageSize
	 * @return the resulting {@link Slice}.
	 * @since 2.1
	 */
	static <T> Slice<T> readSlice(Iterable<Row> rows, @Nullable ByteBuffer pagingState, RowMapper<T> mapper, int page,
			int pageSize) {

		List<T> result = new ArrayList<>(pageSize);

		Iterator<Row> iterator = rows.iterator();
		int index = 0;

		while (iterator.hasNext()) {
			T element = mapper.mapRow(iterator.next(), index++);
			result.add(element);
		}

		CassandraPageRequest pageRequest = CassandraPageRequest.of(PageRequest.of(page, pageSize), pagingState);

		return new SliceImpl<>(result, pageRequest, pagingState != null);
	}

	/**
	 * Extract the table name from a {@link Statement}.
	 *
	 * @param statement
	 * @return
	 * @since 2.1
	 */
	static CqlIdentifier getTableName(Statement<?> statement) {

		String cql = statement instanceof SimpleStatement ? ((SimpleStatement) statement).getQuery() : statement.toString();
		Matcher matcher = FROM_REGEX.matcher(cql);

		if (matcher.find()) {

			String cqlTableName = matcher.group(1);

			int separator = cqlTableName.indexOf('.');

			if (separator != -1) {
				cqlTableName = cqlTableName.substring(separator + 1);
			}

			if (cqlTableName.startsWith("\"") || cqlTableName.endsWith("\"")) {
				return CqlIdentifier.fromCql(cqlTableName.substring(separator + 1));
			}

			return CqlIdentifier.fromInternal(cqlTableName);
		}

		return CqlIdentifier.fromCql("unknown");
	}

	/**
	 * Returns a view containing the first {@code limitSize} elements of {@code iterator}. If {@code
	 * iterator} contains fewer than {@code limitSize} elements, the returned view contains all of its elements. The
	 * returned iterator supports {@code remove()} if {@code iterator} does.
	 *
	 * @param iterator the iterator to limit
	 * @param limitSize the maximum number of elements in the returned iterator
	 * @throws IllegalArgumentException if {@code limitSize} is negative
	 * @since 3.0
	 */
	private static <T> Iterator<T> limit(Iterator<T> iterator, int limitSize) {

		return new Iterator<T>() {
			private int count;

			@Override
			public boolean hasNext() {
				return count < limitSize && iterator.hasNext();
			}

			@Override
			public T next() {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}
				count++;
				return iterator.next();
			}

			@Override
			public void remove() {
				iterator.remove();
			}
		};
	}
}

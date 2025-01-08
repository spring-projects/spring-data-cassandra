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
package org.springframework.data.cassandra.repository.query;

import java.util.Iterator;

import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.query.CassandraScrollPosition;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Sort;
import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Custom {@link org.springframework.data.repository.query.ParameterAccessor} that uses a {@link CassandraConverter} to
 * convert parameters.
 *
 * @author Mark Paluch
 * @see org.springframework.data.cassandra.repository.query.ConvertingParameterAccessor
 * @since 1.5
 */
class ConvertingParameterAccessor implements CassandraParameterAccessor {

	private final CassandraConverter converter;

	private final CassandraParameterAccessor delegate;

	ConvertingParameterAccessor(CassandraConverter converter, CassandraParameterAccessor delegate) {

		this.converter = converter;
		this.delegate = delegate;
	}

	@Override
	public CassandraScrollPosition getScrollPosition() {
		return delegate.getScrollPosition();
	}

	@Override
	public Pageable getPageable() {
		return this.delegate.getPageable();
	}

	@Override
	public Sort getSort() {
		return this.delegate.getSort();
	}

	@Override
	public Limit getLimit() {
		return this.delegate.getLimit();
	}

	@Nullable
	@Override
	public Class<?> findDynamicProjection() {
		return this.delegate.findDynamicProjection();
	}

	@Override
	public Object getBindableValue(int index) {
		return potentiallyConvert(index, this.delegate.getBindableValue(index));
	}

	@Override
	public CassandraType findCassandraType(int index) {
		return this.delegate.findCassandraType(index);
	}

	@Override
	public DataType getDataType(int index) {
		return this.delegate.getDataType(index);
	}

	@Override
	public Class<?> getParameterType(int index) {
		return this.delegate.getParameterType(index);
	}

	@Nullable
	@Override
	public QueryOptions getQueryOptions() {
		return this.delegate.getQueryOptions();
	}

	@Override
	public boolean hasBindableNullValue() {
		return this.delegate.hasBindableNullValue();
	}

	public Iterator<Object> iterator() {
		return new ConvertingIterator(this.delegate.iterator());
	}

	@Override
	public Object[] getValues() {
		return this.delegate.getValues();
	}

	@SuppressWarnings("unchecked")
	@Nullable
	Object potentiallyConvert(int index, @Nullable Object bindableValue) {

		if (bindableValue == null) {
			return null;
		}

		if (bindableValue instanceof Range) {
			return bindableValue;
		}

		CassandraType cassandraType = this.delegate.findCassandraType(index);

		if (cassandraType != null) {
			this.converter.convertToColumnType(bindableValue, converter.getColumnTypeResolver().resolve(cassandraType));
		}

		return this.converter.convertToColumnType(bindableValue, converter.getColumnTypeResolver().resolve(bindableValue));
	}

	/**
	 * Custom {@link Iterator} to convert items before returning them.
	 *
	 * @author Mark Paluch
	 */
	private class ConvertingIterator implements Iterator<Object> {

		private final Iterator<Object> delegate;

		private int index = 0;

		/**
		 * Create a new {@link ConvertingIterator} for the given delegate.
		 *
		 * @param delegate must not be {@literal null}.
		 */
		ConvertingIterator(Iterator<Object> delegate) {
			this.delegate = delegate;
		}

		public boolean hasNext() {
			return this.delegate.hasNext();
		}

		@Nullable
		public Object next() {
			return potentiallyConvert(this.index++, this.delegate.next());
		}

		public void remove() {
			this.delegate.remove();
		}

	}
}

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
package org.springframework.data.cassandra.repository.query;

import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;

import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.domain.Pageable;
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

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#getPageable()
	 */
	@Override
	public Pageable getPageable() {
		return this.delegate.getPageable();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#getSort()
	 */
	@Override
	public Sort getSort() {
		return this.delegate.getSort();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#getDynamicProjection()
	 */
	@Override
	public Optional<Class<?>> getDynamicProjection() {
		return this.delegate.getDynamicProjection();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#findDynamicProjection()
	 */
	@Nullable
	@Override
	public Class<?> findDynamicProjection() {
		return this.delegate.findDynamicProjection();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#getBindableValue(int)
	 */
	@Override
	public Object getBindableValue(int index) {
		return potentiallyConvert(index, this.delegate.getBindableValue(index), null);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.query.CassandraParameterAccessor#findCassandraType(int)
	 */
	@Override
	public CassandraType findCassandraType(int index) {
		return this.delegate.findCassandraType(index);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.query.CassandraParameterAccessor#getDataType(int)
	 */
	@Override
	public DataType getDataType(int index) {
		return this.delegate.getDataType(index);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.query.CassandraParameterAccessor#getParameterType(int)
	 */
	@Override
	public Class<?> getParameterType(int index) {
		return this.delegate.getParameterType(index);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.query.CassandraParameterAccessor#getQueryOptions()
	 */
	@Nullable
	@Override
	public QueryOptions getQueryOptions() {
		return this.delegate.getQueryOptions();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#hasBindableNullValue()
	 */
	@Override
	public boolean hasBindableNullValue() {
		return this.delegate.hasBindableNullValue();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#iterator()
	 */
	public Iterator<Object> iterator() {
		return new ConvertingIterator(this.delegate.iterator());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.query.CassandraParameterAccessor#getValues()
	 */
	@Override
	public Object[] getValues() {
		return this.delegate.getValues();
	}

	@SuppressWarnings("unchecked")
	@Nullable
	Object potentiallyConvert(int index, @Nullable Object bindableValue, @Nullable CassandraPersistentProperty property) {

		if (bindableValue == null) {
			return null;
		}

		CassandraType cassandraType = this.delegate.findCassandraType(index);

		if (cassandraType != null) {
			this.converter.convertToColumnType(bindableValue, converter.getColumnTypeResolver().resolve(cassandraType));
		}

		if (property != null && ((property.isCollectionLike() && bindableValue instanceof Collection)
				|| (!property.isCollectionLike() && !(bindableValue instanceof Collection)))) {
			return this.converter.convertToColumnType(bindableValue, converter.getColumnTypeResolver().resolve(property));
		}

		return this.converter.convertToColumnType(bindableValue, converter.getColumnTypeResolver().resolve(bindableValue));
	}

	/**
	 * Custom {@link Iterator} to convert items before returning them.
	 *
	 * @author Mark Paluch
	 */
	private class ConvertingIterator implements PotentiallyConvertingIterator {

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

		/*
		 * (non-Javadoc)
		 * @see java.util.Iterator#hasNext()
		 */
		public boolean hasNext() {
			return this.delegate.hasNext();
		}

		/*
		 * (non-Javadoc)
		 * @see java.util.Iterator#next()
		 */
		@Nullable
		public Object next() {
			return potentiallyConvert(this.index++, this.delegate.next(), null);
		}

		/*
		 * (non-Javadoc)
		 * @see java.util.Iterator#remove()
		 */
		public void remove() {
			this.delegate.remove();
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.repository.query.ConvertingParameterAccessor.PotentiallyConvertingIterator#nextConverted(org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty)
		 */
		@Nullable
		@Override
		public Object nextConverted(CassandraPersistentProperty property) {
			return potentiallyConvert(this.index++, this.delegate.next(), property);
		}
	}

	/**
	 * Custom {@link Iterator} that adds a method to access elements in a converted manner.
	 *
	 * @author Mark Paluch
	 */
	interface PotentiallyConvertingIterator extends Iterator<Object> {

		/**
		 * Returns the next element and pass in type information for potential conversion.
		 *
		 * @return the converted object, may be {@literal null}.
		 */
		@Nullable
		Object nextConverted(CassandraPersistentProperty property);

	}
}

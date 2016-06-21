/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.cassandra.repository.query;

import java.util.Iterator;

import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;

/**
 * Custom {@link org.springframework.data.repository.query.ParameterAccessor} that uses a {@link CassandraConverter} to
 * convert parameters.
 *
 * @author Mark Paluch
 * @since 1.5
 */
class ConvertingParameterAccessor implements CassandraParameterAccessor {

	private final CassandraConverter cassandraConverter;
	private final CassandraParameterAccessor delegate;

	public ConvertingParameterAccessor(CassandraConverter cassandraConverter, CassandraParameterAccessor delegate) {

		this.cassandraConverter = cassandraConverter;
		this.delegate = delegate;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#getPageable()
	 */
	@Override
	public Pageable getPageable() {
		return delegate.getPageable();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#getSort()
	 */
	@Override
	public Sort getSort() {
		return delegate.getSort();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#getDynamicProjection()
	 */
	@Override
	public Class<?> getDynamicProjection() {
		return delegate.getDynamicProjection();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#getBindableValue(int)
	 */
	@Override
	public Object getBindableValue(int index) {
		return potentiallyConvert(index, delegate.getBindableValue(index));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.query.CassandraParameterAccessor#getDataType(int)
	 */
	@Override
	public DataType getDataType(int index) {
		return delegate.getDataType(index);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.query.CassandraParameterAccessor#getParameterType(int)
	 */
	@Override
	public Class<?> getParameterType(int index) {
		return delegate.getParameterType(index);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#hasBindableNullValue()
	 */
	@Override
	public boolean hasBindableNullValue() {
		return delegate.hasBindableNullValue();
	}

	/*
	  * (non-Javadoc)
	  *
	  * @see java.lang.Iterable#iterator()
	  */
	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#iterator()
	 */
	public Iterator<Object> iterator() {
		return new ConvertingIterator(delegate.iterator());
	}

	private Object potentiallyConvert(int index, Object bindableValue) {

		if (bindableValue == null) {
			return null;
		}

		DataType parameterType = getDataType(index);
		if (parameterType == null) {
			parameterType = cassandraConverter.getMappingContext().getDataType(getParameterType(index));
		}

		TypeCodec<?> cassandraType = CodecRegistry.DEFAULT_INSTANCE.codecFor(parameterType);

		if (cassandraType.getJavaType().getRawType().isAssignableFrom(bindableValue.getClass())) {
			return bindableValue;
		}

		return cassandraConverter.getConversionService().convert(bindableValue, cassandraType.getJavaType().getRawType());
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
		 * Creates a new {@link ConvertingIterator} for the given delegate.
		 *
		 * @param delegate
		 */
		public ConvertingIterator(Iterator<Object> delegate) {
			this.delegate = delegate;
		}

		/*
		 * (non-Javadoc)
		 * @see java.util.Iterator#hasNext()
		 */
		public boolean hasNext() {
			return delegate.hasNext();
		}

		/*
		 * (non-Javadoc)
		 * @see java.util.Iterator#next()
		 */
		public Object next() {
			return potentiallyConvert(index++, next());
		}

		/*
		 * (non-Javadoc)
		 * @see java.util.Iterator#remove()
		 */
		public void remove() {
			delegate.remove();
		}
	}

}

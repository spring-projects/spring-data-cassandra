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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;

import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * Simple {@link ParameterAccessor} that returns the given parameters unfiltered.
 *
 * @author Mark Paluch
 */
class StubParameterAccessor implements CassandraParameterAccessor {

	private final Object[] values;

	/**
	 * Create a new {@link ConvertingParameterAccessor} backed by a {@link StubParameterAccessor} simply returning the
	 * given parameters converted but unfiltered.
	 *
	 * @param converter
	 * @param parameters
	 * @return
	 */
	public static ConvertingParameterAccessor getAccessor(CassandraConverter converter, Object... parameters) {
		return new ConvertingParameterAccessor(converter, new StubParameterAccessor(parameters));
	}

	@SuppressWarnings("unchecked")
	private StubParameterAccessor(Object... values) {
		this.values = values;
	}

	@Override
	public DataType getDataType(int index) {
		return CodecRegistry.DEFAULT.codecFor(values[index]).getCqlType();
	}

	@Override
	public Class<?> getParameterType(int index) {
		return values[index].getClass();
	}

	@Nullable
	@Override
	public QueryOptions getQueryOptions() {
		return null;
	}

	@Override
	public Pageable getPageable() {
		return null;
	}

	@Override
	public Sort getSort() {
		return null;
	}

	@Override
	public Optional<Class<?>> getDynamicProjection() {
		return Optional.empty();
	}

	@Nullable
	@Override
	public Class<?> findDynamicProjection() {
		return null;
	}

	@Override
	public Object getBindableValue(int index) {
		return values[index];
	}

	@Override
	public boolean hasBindableNullValue() {
		return false;
	}

	@Override
	public Iterator<Object> iterator() {
		return Arrays.asList(values).iterator();
	}

	@Override
	public CassandraType findCassandraType(int index) {
		return null;
	}

	@Override
	public Object[] getValues() {
		return new Object[0];
	}
}

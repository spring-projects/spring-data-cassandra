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

import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.mapping.CassandraSimpleTypeHolder;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.query.CassandraScrollPosition;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Cassandra-specific {@link ParameterAccessor} exposing Cassandra {@link DataType types} that are supported by the
 * driver and parameter type.
 *
 * @author Mark Paluch
 * @see org.springframework.data.cassandra.repository.query.CassandraParameterAccessor
 * @see org.springframework.data.repository.query.ParametersParameterAccessor
 */
public class CassandraParametersParameterAccessor extends ParametersParameterAccessor
		implements CassandraParameterAccessor {

	/**
	 * Create a new {@link CassandraParametersParameterAccessor}.
	 *
	 * @param method must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 */
	public CassandraParametersParameterAccessor(CassandraQueryMethod method, Object... values) {

		super(method.getParameters(), values);
	}

	@Override
	public DataType getDataType(int index) {

		CassandraType cassandraType = findCassandraType(index);

		return (cassandraType != null ? CassandraSimpleTypeHolder.getDataTypeFor(cassandraType.type())
				: CassandraSimpleTypeHolder.getDataTypeFor(getParameterType(index)));
	}

	@Nullable
	@Override
	public CassandraType findCassandraType(int index) {
		return getParameters().getParameter(index).getCassandraType();
	}

	@Override
	public Class<?> getParameterType(int index) {
		return getParameters().getParameter(index).getType();
	}

	@Override
	public CassandraParameters getParameters() {
		return (CassandraParameters) super.getParameters();
	}

	@Override
	public Object[] getValues() {
		return super.getValues();
	}

	@Override
	public CassandraScrollPosition getScrollPosition() {

		ScrollPosition scrollPosition = super.getScrollPosition();
		if (scrollPosition instanceof CassandraScrollPosition csp) {
			return csp;
		}

		if (scrollPosition == null) {
			return CassandraScrollPosition.initial();
		}

		throw new IllegalArgumentException(
				"Unsupported scroll position " + scrollPosition + ". Only CassandraScrollPosition supported.");
	}

	@Override
	public Limit getLimit() {

		if (!getParameters().hasLimitParameter()) {
			return Limit.unlimited();
		}

		return super.getLimit();
	}

	@Nullable
	@Override
	public QueryOptions getQueryOptions() {

		int queryOptionsIndex = getParameters().getQueryOptionsIndex();

		Object value = (queryOptionsIndex != -1 ? getValue(queryOptionsIndex) : null);

		return (QueryOptions) value;
	}
}

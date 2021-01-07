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

import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Cassandra-specific {@link ParameterAccessor} exposing a Cassandra {@link DataType types} that are supported by the
 * driver and parameter type.
 *
 * @author Matthew Adams
 * @author Mark Paluch
 */
public interface CassandraParameterAccessor extends ParameterAccessor {

	/**
	 * Returns the {@link CassandraType} for the declared method parameter.
	 *
	 * @param index the parameter index
	 * @return the Cassandra {@link CassandraType} or {@literal null}.
	 * @see org.springframework.data.cassandra.core.mapping.CassandraSimpleTypeHolder
	 * @see org.springframework.data.cassandra.core.mapping.CassandraType
	 */
	@Nullable
	CassandraType findCassandraType(int index);

	/**
	 * Returns the Cassandra {@link DataType} for the declared parameter if the type is a
	 * {@link org.springframework.data.cassandra.core.mapping.CassandraSimpleTypeHolder simple type}. Parameter types may
	 * be specified using {@link org.springframework.data.cassandra.core.mapping.CassandraType}.
	 *
	 * @param index the parameter index
	 * @return the Cassandra {@link DataType} or {@literal null} if the parameter type cannot be determined from
	 *         {@link org.springframework.data.cassandra.core.mapping.CassandraSimpleTypeHolder}
	 * @see org.springframework.data.cassandra.core.mapping.CassandraSimpleTypeHolder
	 * @see org.springframework.data.cassandra.core.mapping.CassandraType
	 */
	DataType getDataType(int index);

	/**
	 * The actual parameter type (after unwrapping).
	 *
	 * @param index the parameter index
	 * @return the parameter type, never {@literal null}.
	 */
	Class<?> getParameterType(int index);

	/**
	 * Returns the raw parameter values of the underlying query method.
	 *
	 * @return the raw parameter values passed to the underlying query method.
	 * @since 1.5
	 */
	Object[] getValues();

	/**
	 * Returns the {@link QueryOptions} associated with the associated Repository query method.
	 *
	 * @return the {@link QueryOptions} or {@literal null} if none.
	 * @since 2.0
	 */
	@Nullable
	QueryOptions getQueryOptions();

}

/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.cassandra.core.convert;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;

/**
 * Descriptor for a Cassandra column type exposing a {@link DataType}.
 *
 * @author Mark Paluch
 * @since 3.0
 */
public interface CassandraColumnType extends ColumnType {

	/**
	 * Returns the {@link DataType} associated with this column type.
	 *
	 * @return
	 * @throws org.springframework.data.mapping.MappingException if the column cannot be mapped onto a Cassandra type.
	 */
	DataType getDataType();

	/**
	 * Returns whether the associated {@link DataType} is a {@link TupleType}.
	 *
	 * @return
	 */
	default boolean isTupleType() {
		return getDataType() instanceof TupleType;
	}

	/**
	 * Returns whether the associated {@link DataType} is a {@link UserDefinedType}.
	 *
	 * @return
	 */
	default boolean isUserDefinedType() {
		return getDataType() instanceof UserDefinedType;
	}
}

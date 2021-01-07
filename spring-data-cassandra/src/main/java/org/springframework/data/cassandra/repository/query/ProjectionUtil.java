/*
 * Copyright 2018-2021 the original author or authors.
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
import java.util.HashSet;
import java.util.Set;

import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;

/**
 * Utility methods for projections.
 *
 * @author Mark Paluch
 * @since 2.1
 */
abstract class ProjectionUtil {

	private final static Set<DataType> NUMERIC_TYPES = new HashSet<>(Arrays.asList(DataTypes.BIGINT, DataTypes.VARINT,
			DataTypes.SMALLINT, DataTypes.INT, DataTypes.COUNTER, DataTypes.TINYINT));

	private ProjectionUtil() {}

	/**
	 * Determine whether multiple {@code boolean} flags are set. Allowed is at most a single {@literal true} value.
	 *
	 * @param flags
	 * @return {@literal true} if more than one {@code flag} is set to {@literal true}.
	 */
	static boolean hasAmbiguousProjectionFlags(Boolean... flags) {
		return Arrays.stream(flags).filter(Boolean::booleanValue).count() > 1;
	}

	/**
	 * Determine whether the {@link Row} qualifies as a count projection. Count projection candidates have a single
	 * numeric column.
	 *
	 * @param row {@link Row} to evaluate for a count projection.
	 * @return a boolean value indicating whether the {@link Row} qualifies as a count projection.
	 */
	static boolean qualifiesAsCountProjection(Row row) {

		ColumnDefinitions columnDefinitions = row.getColumnDefinitions();

		return columnDefinitions.size() == 1 && NUMERIC_TYPES.contains(columnDefinitions.get(0).getType());
	}
}

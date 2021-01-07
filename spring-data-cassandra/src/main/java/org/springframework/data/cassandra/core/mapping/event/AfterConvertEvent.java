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
package org.springframework.data.cassandra.core.mapping.event;

import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;

/**
 * Event to be triggered after converting a {@link Row}.
 *
 * @author Mark Paluch
 * @since 2.1
 */
public class AfterConvertEvent<E> extends CassandraMappingEvent<E> {

	private static final long serialVersionUID = 1L;

	private final Row row;

	/**
	 * Creates a new {@link AfterConvertEvent} for the given {@code source} and {@link CqlIdentifier tableName}.
	 *
	 * @param source must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 */
	public AfterConvertEvent(Row row, E source, CqlIdentifier tableName) {

		super(source, tableName);

		Assert.notNull(row, "Row must not be null");
		this.row = row;
	}

	/**
	 * Returns the {@link Row} from which this {@link AfterConvertEvent} was derived.
	 *
	 * @return
	 */
	public Row getRow() {
		return row;
	}
}

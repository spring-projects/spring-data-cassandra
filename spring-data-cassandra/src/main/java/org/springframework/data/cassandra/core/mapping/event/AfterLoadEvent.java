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
 * Event to be triggered after loading {@link com.datastax.driver.core.Row}s to be mapped onto a given type.
 *
 * @author Lukasz Antoniak
 * @author Mark Paluch
 * @since 2.1
 */
public class AfterLoadEvent<T> extends CassandraMappingEvent<Row> {

	private static final long serialVersionUID = 1L;

	private final Class<T> type;

	/**
	 * Creates a new {@link AfterLoadEvent} for the given {@link Row}, type and {@link CqlIdentifier tableName}.
	 *
	 * @param source must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 */
	public AfterLoadEvent(Row source, Class<T> type, CqlIdentifier tableName) {

		super(source, tableName);

		Assert.notNull(type, "Type must not be null!");
		this.type = type;
	}

	/**
	 * Returns the type for which the {@link AfterLoadEvent} shall be invoked for.
	 *
	 * @return
	 */
	public Class<T> getType() {
		return type;
	}
}

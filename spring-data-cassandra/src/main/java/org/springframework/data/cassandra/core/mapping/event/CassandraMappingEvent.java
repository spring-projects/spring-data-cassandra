/*
 * Copyright 2018-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping.event;

import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Base {@link ApplicationEvent} triggered by Spring Data Cassandra.
 *
 * @author Lukasz Antoniak
 * @author Mark Paluch
 * @since 2.1
 */
public class CassandraMappingEvent<T> extends ApplicationEvent {

	private static final long serialVersionUID = 1L;

	private final CqlIdentifier tableName;

	/**
	 * Creates new {@link CassandraMappingEvent}.
	 *
	 * @param source must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 */
	public CassandraMappingEvent(T source, CqlIdentifier tableName) {

		super(source);

		Assert.notNull(tableName, "Table name must not be null!");
		this.tableName = tableName;
	}

	/**
	 * @return table name that event refers to. May return {@literal null} for non-entity objects.
	 */
	public CqlIdentifier getTableName() {
		return tableName;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.EventObject#getSource()
	 */
	@SuppressWarnings({ "unchecked" })
	@Override
	public T getSource() {
		return (T) super.getSource();
	}
}

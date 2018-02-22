/*
 * Copyright 2013-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping.event;

import org.springframework.context.ApplicationEvent;
import org.springframework.lang.Nullable;

/**
 * Base {@link ApplicationEvent} triggered by Spring Data Cassandra.
 *
 * @author Lukasz Antoniak
 */
public class CassandraMappingEvent<T> extends ApplicationEvent {
	private static final long serialVersionUID = 1L;
	private final @Nullable String table;

	/**
	 * Creates new {@link CassandraMappingEvent}.
	 *
	 * @param source must not be {@literal null}.
	 * @param table may be {@literal null}.
	 */
	public CassandraMappingEvent(T source, @Nullable String table) {
		super(source);
		this.table = table;
	}

	/**
	 * @return Table that event refers to. May return {@literal null} for not entity objects.
	 */
	@Nullable
	public String getTable() {
		return table;
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

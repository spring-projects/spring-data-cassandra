/*
 * Copyright 2018-2025 the original author or authors.
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

import java.io.Serial;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * {@link CassandraMappingEvent} triggered after save of an object.
 *
 * @author Lukasz Antoniak
 * @author Mark Paluch
 * @since 2.1
 */
public class AfterSaveEvent<E> extends CassandraMappingEvent<E> {

	@Serial private static final long serialVersionUID = 1L;

	/**
	 * Creates a new {@link AfterSaveEvent}.
	 *
	 * @param source must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 */
	public AfterSaveEvent(E source, CqlIdentifier tableName) {
		super(source, tableName);
	}
}

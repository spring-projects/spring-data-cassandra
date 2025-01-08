/*
 * Copyright 2019-2025 the original author or authors.
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

import org.springframework.data.mapping.callback.EntityCallback;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * Callback being invoked before a domain object is converted to be persisted. Entity callback invoked before converting
 * a domain object to a {@code INSERT}/{@code UPDATE} {@link Statement}. This is useful to apply changes to the domain
 * objects to that these will be reflected in the generated {@link Statement}.
 *
 * @author Mark Paluch
 * @since 2.2
 * @see org.springframework.data.mapping.callback.EntityCallbacks
 * @see BeforeSaveCallback
 */
@FunctionalInterface
public interface BeforeConvertCallback<T> extends EntityCallback<T> {

	/**
	 * Entity callback method invoked before a domain object is converted to be persisted. Can return either the same or a
	 * modified instance of the domain object.
	 *
	 * @param entity the domain object to save.
	 * @param tableName name of the table.
	 * @return the domain object to be persisted.
	 */
	T onBeforeConvert(T entity, CqlIdentifier tableName);
}

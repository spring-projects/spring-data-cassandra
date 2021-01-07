/*
 * Copyright 2019-2021 the original author or authors.
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
 * Entity callback triggered before save of a row.
 *
 * @author Mark Paluch
 * @since 2.2
 * @see org.springframework.data.mapping.callback.EntityCallbacks
 */
@FunctionalInterface
public interface BeforeSaveCallback<T> extends EntityCallback<T> {
	// TODO: Mutable statements
	/**
	 * Entity callback method invoked before a domain object is saved. Can return either the same of a modified instance
	 * of the domain object and can modify {@link Statement} contents. This method is called after converting the
	 * {@code entity} to {@link Statement} so effectively the row is used as outcome of invoking this callback.
	 *
	 * @param entity the domain object to save.
	 * @param tableName name of the table.
	 * @param statement {@link Statement} representing the {@code entity} operation.
	 * @return the domain object to be persisted.
	 */
	T onBeforeSave(T entity, CqlIdentifier tableName, Statement<?> statement);
}

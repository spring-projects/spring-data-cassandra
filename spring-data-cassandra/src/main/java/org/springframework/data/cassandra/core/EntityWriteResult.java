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
package org.springframework.data.cassandra.core;

import java.util.List;

import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

/**
 * The result of a write operation for an entity.
 *
 * @author Mark Paluch
 * @since 2.1
 * @see WriteResult
 */
public class EntityWriteResult<T> extends WriteResult {

	private final T entity;

	EntityWriteResult(List<ExecutionInfo> executionInfo, boolean wasApplied, List<Row> rows, T entity) {
		super(executionInfo, wasApplied, rows);
		this.entity = entity;
	}

	EntityWriteResult(ResultSet resultSet, T entity) {
		super(resultSet);
		this.entity = entity;
	}

	/**
	 * Create a {@link EntityWriteResult} from {@link WriteResult} and an entity.
	 *
	 * @param result must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return the {@link EntityWriteResult} for {@link WriteResult} and an entity.
	 */
	static <T> EntityWriteResult<T> of(WriteResult result, T entity) {
		return new EntityWriteResult<>(result.getExecutionInfo(), result.wasApplied(), result.getRows(), entity);
	}

	/**
	 * Create a {@link EntityWriteResult} from {@link ResultSet} and an entity.
	 *
	 * @param resultSet must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return the {@link EntityWriteResult} for {@link ResultSet} and an entity.
	 */
	static <T> EntityWriteResult<T> of(ResultSet resultSet, T entity) {
		return new EntityWriteResult<>(resultSet, entity);
	}

	/**
	 * @return the entity associated with this write operation result.
	 */
	public T getEntity() {
		return entity;
	}
}

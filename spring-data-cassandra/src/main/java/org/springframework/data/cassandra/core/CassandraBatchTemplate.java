/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.util.Assert;

import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Default implementation for {@link CassandraBatchOperations}.
 *
 * @author Mark Paluch
 * @author John Blum
 * @since 1.5
 */
class CassandraBatchTemplate implements CassandraBatchOperations {

	static final Object[] EMPTY_ARRAY = new Object[0];

	private AtomicBoolean executed = new AtomicBoolean();
	private final Batch batch;
	private final CassandraOperations operations;

	/**
	 * Creates a new {@link CassandraBatchTemplate} given {@link CassandraOperations}.
	 * 
	 * @param operations must not be {@literal null}.
	 */
	public CassandraBatchTemplate(CassandraOperations operations) {

		Assert.notNull(operations, "CassandraOperations must not be null");

		this.operations = operations;
		this.batch = QueryBuilder.batch();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#execute()
	 */
	@Override
	public void execute() {

		if (executed.compareAndSet(false, true)) {
			operations.getCqlOperations().execute(batch);
			return;
		}

		assertNotExecuted();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#withTimestamp(long)
	 */
	@Override
	public CassandraBatchOperations withTimestamp(long timestamp) {

		assertNotExecuted();

		batch.using(QueryBuilder.timestamp(timestamp));

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#insert(Object...)
	 */
	@Override
	public CassandraBatchOperations insert(Object... entities) {
		return insert(nullSafeIterable(entities));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#insert(java.lang.Iterable)
	 */
	@Override
	public CassandraBatchOperations insert(Iterable<?> entities) {

		assertNotExecuted();

		for (Object entity : nullSafeIterable(entities)) {
			Assert.notNull(entity, "Entity must not be null");
			batch.add(QueryUtils.createInsertQuery(getTableName(entity), entity, null, operations.getConverter()));
		}

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#update(Object...)
	 */
	@Override
	public CassandraBatchOperations update(Object... entities) {
		return update(nullSafeIterable(entities));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#update(java.lang.Iterable)
	 */
	@Override
	public CassandraBatchOperations update(Iterable<?> entities) {

		assertNotExecuted();

		for (Object entity : nullSafeIterable(entities)) {
			Assert.notNull(entity, "Entity must not be null");
			batch.add(QueryUtils.createUpdateQuery(getTableName(entity), entity, null, operations.getConverter()));
		}

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#delete(Object...)
	 */
	@Override
	public CassandraBatchOperations delete(Object... entities) {
		return delete(nullSafeIterable(entities));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#delete(java.lang.Iterable)
	 */
	@Override
	public CassandraBatchOperations delete(Iterable<?> entities) {

		assertNotExecuted();

		for (Object entity : nullSafeIterable(entities)) {
			Assert.notNull(entity, "Entity must not be null");
			batch.add(QueryUtils.createDeleteQuery(getTableName(entity), entity, null, operations.getConverter()));
		}

		return this;
	}

	private void assertNotExecuted() {
		Assert.state(!executed.get(), "This Cassandra Batch was already executed");
	}

	private String getTableName(Object entity) {

		Assert.notNull(entity, "Entity must not be null");
		return operations.getTableName(entity.getClass()).toCql();
	}

	private <T> Iterable<T> nullSafeIterable(T... array) {
		return (array == null ? Collections.<T> emptyList() : Arrays.asList(array));
	}

	private <T> Iterable<T> nullSafeIterable(Iterable<T> iterable) {
		return (iterable != null ? iterable : Collections.<T> emptyList());
	}
}

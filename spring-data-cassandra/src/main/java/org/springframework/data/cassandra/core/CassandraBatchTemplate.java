/*
 * Copyright 2016-2018 the original author or authors.
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
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.util.Assert;

import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Default implementation for {@link CassandraBatchOperations}.
 *
 * @author Mark Paluch
 * @author John Blum
 * @author Anup Sabbi
 * @since 1.5
 */
class CassandraBatchTemplate implements CassandraBatchOperations {

	private AtomicBoolean executed = new AtomicBoolean();

	private final Batch batch;

	private final CassandraOperations operations;

	/**
	 * Create a new {@link CassandraBatchTemplate} given {@link CassandraOperations}.
	 *
	 * @param operations must not be {@literal null}.
	 */
	CassandraBatchTemplate(CassandraOperations operations) {

		Assert.notNull(operations, "CassandraOperations must not be null");

		this.operations = operations;
		this.batch = QueryBuilder.batch();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#execute()
	 */
	@Override
	public WriteResult execute() {

		if (executed.compareAndSet(false, true)) {
			return WriteResult.of(operations.getCqlOperations().queryForResultSet(batch));
		}

		throw new IllegalStateException("This Cassandra Batch was already executed");
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#withTimestamp(long)
	 */
	@Override
	public CassandraBatchOperations withTimestamp(long timestamp) {

		assertNotExecuted();

		batch.using(QueryBuilder.timestamp(timestamp));

		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#insert(java.lang.Object[])
	 */
	@Override
	public CassandraBatchOperations insert(Object... entities) {

		Assert.notNull(entities, "Entities must not be null");

		return insert(Arrays.asList(entities));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#insert(java.lang.Iterable)
	 */
	@Override
	public CassandraBatchOperations insert(Iterable<?> entities) {
		return insert(entities, InsertOptions.empty());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#insert(java.lang.Iterable, org.springframework.data.cassandra.core.cql.WriteOptions)
	 */
	@Override
	public CassandraBatchOperations insert(Iterable<?> entities, WriteOptions options) {

		assertNotExecuted();
		Assert.notNull(entities, "Entities must not be null");
		Assert.notNull(options, "WriteOptions must not be null");

		for (Object entity : entities) {

			Assert.notNull(entity, "Entity must not be null");
			batch.add(QueryUtils.createInsertQuery(getTableName(entity), entity, options, operations.getConverter()));
		}

		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#update(java.lang.Object[])
	 */
	@Override
	public CassandraBatchOperations update(Object... entities) {

		Assert.notNull(entities, "Entities must not be null");

		return update(Arrays.asList(entities));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#update(java.lang.Iterable)
	 */
	@Override
	public CassandraBatchOperations update(Iterable<?> entities) {
		return update(entities, UpdateOptions.empty());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#update(java.lang.Iterable, org.springframework.data.cassandra.core.cql.WriteOptions)
	 */
	@Override
	public CassandraBatchOperations update(Iterable<?> entities, WriteOptions options) {

		assertNotExecuted();
		Assert.notNull(entities, "Entities must not be null");
		Assert.notNull(options, "WriteOptions must not be null");

		for (Object entity : entities) {

			Assert.notNull(entity, "Entity must not be null");
			batch.add(QueryUtils.createUpdateQuery(getTableName(entity), entity, options, operations.getConverter()));
		}

		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#delete(java.lang.Object[])
	 */
	@Override
	public CassandraBatchOperations delete(Object... entities) {

		Assert.notNull(entities, "Entities must not be null");

		return delete(Arrays.asList(entities));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#delete(java.lang.Iterable)
	 */
	@Override
	public CassandraBatchOperations delete(Iterable<?> entities) {

		assertNotExecuted();
		Assert.notNull(entities, "Entities must not be null");

		for (Object entity : entities) {
			Assert.notNull(entity, "Entity must not be null");
			batch.add(
					QueryUtils.createDeleteQuery(getTableName(entity), entity, QueryOptions.empty(), operations.getConverter()));
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
}

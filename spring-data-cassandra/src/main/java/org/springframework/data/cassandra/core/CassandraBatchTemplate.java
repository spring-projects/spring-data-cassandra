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

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.util.Assert;

import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Default implementation for {@link CassandraBatchOperations}.
 *
 * @author Mark Paluch
 * @since 1.5
 */
class CassandraBatchTemplate implements CassandraBatchOperations {

	private final CassandraTemplate cassandraTemplate;
	private final Batch batch;
	private AtomicBoolean executed = new AtomicBoolean();

	public CassandraBatchTemplate(CassandraTemplate cassandraTemplate) {

		Assert.notNull(cassandraTemplate, "CassandraTemplate must not be null");

		this.cassandraTemplate = cassandraTemplate;
		this.batch = QueryBuilder.batch();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#execute()
	 */
	@Override
	public void execute() {

		if (executed.compareAndSet(false, true)) {
			cassandraTemplate.execute(batch);
			return;
		}

		ensureNotExecuted();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#withTimestamp(long)
	 */
	@Override
	public CassandraBatchOperations withTimestamp(long timestamp) {

		ensureNotExecuted();

		batch.using(QueryBuilder.timestamp(timestamp));
		return this;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#insert(java.lang.Object)
	 */
	@Override
	public CassandraBatchOperations insert(Object entity) {

		ensureNotExecuted();
		Assert.notNull(entity, "Entity must not be null");

		batch.add(cassandraTemplate.createInsertQuery(entity, null));
		return this;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#insert(java.lang.Iterable)
	 */
	@Override
	public CassandraBatchOperations insert(Iterable<? extends Object> entities) {

		ensureNotExecuted();
		Assert.notNull(entities, "Entities must not be null");

		for (Object entity : entities) {

			Assert.notNull(entity, "Entity must not be null");
			batch.add(cassandraTemplate.createInsertQuery(entity, null));
		}

		return this;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#update(java.lang.Object)
	 */
	@Override
	public CassandraBatchOperations update(Object entity) {

		ensureNotExecuted();
		Assert.notNull(entity, "Entity must not be null");

		batch.add(cassandraTemplate.createUpdateQuery(entity, null));

		return this;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#update(java.lang.Iterable)
	 */
	@Override
	public CassandraBatchOperations update(Iterable<? extends Object> entities) {

		ensureNotExecuted();
		Assert.notNull(entities, "Entities must not be null");

		for (Object entity : entities) {

			Assert.notNull(entity, "Entity must not be null");
			batch.add(cassandraTemplate.createUpdateQuery(entity, null));
		}

		return this;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#delete(java.lang.Object)
	 */
	@Override
	public CassandraBatchOperations delete(Object entity) {

		ensureNotExecuted();
		Assert.notNull(entity, "Entity must not be null");

		batch.add(cassandraTemplate.createDeleteQuery(entity, null));

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#delete(java.lang.Iterable)
	 */
	@Override
	public CassandraBatchOperations delete(Iterable<? extends Object> entities) {

		ensureNotExecuted();
		Assert.notNull(entities, "Entities must not be null");

		for (Object entity : entities) {

			Assert.notNull(entity, "Entity must not be null");
			batch.add(cassandraTemplate.createDeleteQuery(entity, null));
		}

		return this;
	}

	private void ensureNotExecuted() {
		Assert.state(!executed.get(), "This Cassandra Batch was already executed");
	}
}

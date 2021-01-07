/*
 * Copyright 2016-2021 the original author or authors.
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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.UpdateMapper;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * Default implementation for {@link CassandraBatchOperations}.
 *
 * @author Mark Paluch
 * @author John Blum
 * @author Anup Sabbi
 * @since 1.5
 */
class CassandraBatchTemplate implements CassandraBatchOperations {

	private final AtomicBoolean executed = new AtomicBoolean();

	private final BatchStatementBuilder batch = BatchStatement.builder(BatchType.LOGGED);

	private final CassandraConverter converter;

	private final CassandraMappingContext mappingContext;

	private final CassandraOperations operations;

	private final StatementFactory statementFactory;

	/**
	 * Create a new {@link CassandraBatchTemplate} given {@link CassandraOperations}.
	 *
	 * @param operations must not be {@literal null}.
	 */
	CassandraBatchTemplate(CassandraOperations operations) {

		Assert.notNull(operations, "CassandraOperations must not be null");

		this.operations = operations;
		this.converter = operations.getConverter();
		this.mappingContext = this.converter.getMappingContext();
		this.statementFactory = new StatementFactory(new UpdateMapper(converter));
	}

	/**
	 * Return a reference to the configured {@link CassandraConverter} used to map {@link Object Objects} to
	 * {@link com.datastax.driver.core.Row Rows}.
	 *
	 * @return a reference to the configured {@link CassandraConverter}.
	 * @see org.springframework.data.cassandra.core.convert.CassandraConverter
	 */
	protected CassandraConverter getConverter() {
		return this.converter;
	}

	/**
	 * Returns a reference to the configured {@link CassandraMappingContext} used to map entities to Cassandra tables and
	 * back.
	 *
	 * @return a reference to the configured {@link CassandraMappingContext}.
	 * @see org.springframework.data.cassandra.core.mapping.CassandraMappingContext
	 */
	protected CassandraMappingContext getMappingContext() {
		return this.mappingContext;
	}

	/**
	 * Return a reference to the configured {@link StatementFactory} used to create Cassandra {@link Statement} objects to
	 * perform data access operations on a Cassandra cluster.
	 *
	 * @return a reference to the configured {@link StatementFactory}.
	 * @see org.springframework.data.cassandra.core.StatementFactory
	 */
	protected StatementFactory getStatementFactory() {
		return this.statementFactory;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#execute()
	 */
	@Override
	public WriteResult execute() {

		if (this.executed.compareAndSet(false, true)) {
			return WriteResult.of(this.operations.getCqlOperations().queryForResultSet(batch.build()));
		}

		throw new IllegalStateException("This Cassandra Batch was already executed");
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#withTimestamp(long)
	 */
	@Override
	public CassandraBatchOperations withTimestamp(long timestamp) {

		assertNotExecuted();

		this.batch.setQueryTimestamp(timestamp);

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

		CassandraMappingContext mappingContext = getMappingContext();

		for (Object entity : entities) {

			Assert.notNull(entity, "Entity must not be null");

			BasicCassandraPersistentEntity<?> persistentEntity = mappingContext
					.getRequiredPersistentEntity(entity.getClass());

			SimpleStatement insertQuery = getStatementFactory()
					.insert(entity, options, persistentEntity, persistentEntity.getTableName()).build();

			this.batch.addStatement(insertQuery);
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

			CassandraPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(entity.getClass());

			SimpleStatement update = getStatementFactory()
					.update(entity, options, persistentEntity, persistentEntity.getTableName()).build();

			this.batch.addStatement(update);
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
		return delete(entities, DeleteOptions.empty());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraBatchOperations#delete(java.lang.Iterable, org.springframework.data.cassandra.core.cql.WriteOptions)
	 */
	@Override
	public CassandraBatchOperations delete(Iterable<?> entities, WriteOptions options) {

		assertNotExecuted();

		Assert.notNull(entities, "Entities must not be null");
		Assert.notNull(options, "WriteOptions must not be null");

		for (Object entity : entities) {

			Assert.notNull(entity, "Entity must not be null");

			CassandraPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(entity.getClass());

			SimpleStatement delete = getStatementFactory()
					.delete(entity, options, this.getConverter(), persistentEntity.getTableName()).build();

			this.batch.addStatement(delete);
		}

		return this;
	}

	private void assertNotExecuted() {
		Assert.state(!this.executed.get(), "This Cassandra Batch was already executed");
	}

	private CassandraPersistentEntity<?> getRequiredPersistentEntity(Class<?> entityType) {
		return getMappingContext().getRequiredPersistentEntity(ClassUtils.getUserClass(entityType));
	}
}

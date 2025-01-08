/*
 * Copyright 2016-2025 the original author or authors.
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
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.QueryOptionsUtil;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * Default implementation for {@link CassandraBatchOperations}.
 *
 * @author Mark Paluch
 * @author John Blum
 * @author Anup Sabbi
 * @author Sam Lightfoot
 * @since 1.5
 */
class CassandraBatchTemplate implements CassandraBatchOperations {

	private final AtomicBoolean executed = new AtomicBoolean();

	private final BatchStatementBuilder batch;

	private final CassandraConverter converter;

	private final CassandraMappingContext mappingContext;

	private final CassandraOperations operations;

	private final StatementFactory statementFactory;

	private QueryOptions options = QueryOptions.empty();

	/**
	 * Create a new {@link CassandraBatchTemplate} given {@link CassandraOperations} and {@link BatchType}.
	 *
	 * @param operations must not be {@literal null}.
	 * @param batchType must not be {@literal null}.
	 * @since 3.2.6
	 */
	CassandraBatchTemplate(CassandraTemplate operations, BatchType batchType) {

		Assert.notNull(operations, "CassandraOperations must not be null");
		Assert.notNull(batchType, "BatchType must not be null");

		this.operations = operations;
		this.batch = BatchStatement.builder(batchType);
		this.converter = operations.getConverter();
		this.mappingContext = this.converter.getMappingContext();
		this.statementFactory = operations.getStatementFactory();
	}

	/**
	 * Return a reference to the configured {@link CassandraConverter} used to map {@link Object Objects} to
	 * {@link com.datastax.oss.driver.api.core.cql.Row Rows}.
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

	@Override
	public WriteResult execute() {

		if (this.executed.compareAndSet(false, true)) {
			BatchStatement statement = QueryOptionsUtil.addQueryOptions(batch.build(), this.options);
			return WriteResult.of(this.operations.getCqlOperations().queryForResultSet(statement));
		}

		throw new IllegalStateException("This Cassandra Batch was already executed");
	}

	@Override
	public CassandraBatchOperations withTimestamp(long timestamp) {

		assertNotExecuted();

		this.batch.setQueryTimestamp(timestamp);

		return this;
	}

	@Override
	public CassandraBatchOperations withQueryOptions(QueryOptions options) {

		assertNotExecuted();
		Assert.notNull(options, "QueryOptions must not be null");

		this.options = options;

		return this;
	}

	@Override
	public CassandraBatchOperations addStatement(BatchableStatement<?> statement) {

		assertNotExecuted();
		Assert.notNull(statement, "Statement must not be null");

		this.batch.addStatement(statement);

		return this;
	}

	@Override
	public CassandraBatchOperations addStatements(BatchableStatement<?>... statements) {

		assertNotExecuted();
		Assert.notNull(statements, "Statements must not be null");

		this.batch.addStatements(statements);

		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public CassandraBatchOperations addStatements(Iterable<? extends BatchableStatement<?>> statements) {

		assertNotExecuted();
		Assert.notNull(statements, "Statements must not be null");

		this.batch.addStatements((Iterable<BatchableStatement<?>>) statements);

		return this;
	}

	@Override
	public CassandraBatchOperations insert(Object... entities) {

		Assert.notNull(entities, "Entities must not be null");

		return insert(Arrays.asList(entities));
	}

	@Override
	public CassandraBatchOperations insert(Iterable<?> entities) {
		return insert(entities, InsertOptions.empty());
	}

	@Override
	public CassandraBatchOperations insert(Iterable<?> entities, WriteOptions options) {

		assertNotExecuted();
		Assert.notNull(entities, "Entities must not be null");
		Assert.notNull(options, "WriteOptions must not be null");
		assertNotStatement("insert", entities);

		CassandraMappingContext mappingContext = getMappingContext();

		for (Object entity : entities) {

			Assert.notNull(entity, "Entity must not be null");
			assertNotStatement("insert", entity);
			assertNotQueryOptions(entity);

			BasicCassandraPersistentEntity<?> persistentEntity = mappingContext
					.getRequiredPersistentEntity(entity.getClass());

			SimpleStatement insertQuery = getStatementFactory()
					.insert(entity, options, persistentEntity, persistentEntity.getTableName()).build();

			addStatement(insertQuery);
		}

		return this;
	}

	@Override
	public CassandraBatchOperations update(Object... entities) {

		Assert.notNull(entities, "Entities must not be null");

		return update(Arrays.asList(entities));
	}

	@Override
	public CassandraBatchOperations update(Iterable<?> entities) {
		return update(entities, UpdateOptions.empty());
	}

	@Override
	public CassandraBatchOperations update(Iterable<?> entities, WriteOptions options) {

		assertNotExecuted();
		Assert.notNull(entities, "Entities must not be null");
		Assert.notNull(options, "WriteOptions must not be null");

		for (Object entity : entities) {

			Assert.notNull(entity, "Entity must not be null");
			assertNotStatement("update", entity);
			assertNotQueryOptions(entity);

			CassandraPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(entity.getClass());

			SimpleStatement update = getStatementFactory()
					.update(entity, options, persistentEntity, persistentEntity.getTableName()).build();

			addStatement(update);
		}

		return this;
	}

	@Override
	public CassandraBatchOperations delete(Object... entities) {

		Assert.notNull(entities, "Entities must not be null");

		return delete(Arrays.asList(entities));
	}

	@Override
	public CassandraBatchOperations delete(Iterable<?> entities) {
		return delete(entities, DeleteOptions.empty());
	}

	@Override
	public CassandraBatchOperations delete(Iterable<?> entities, WriteOptions options) {

		assertNotExecuted();
		Assert.notNull(entities, "Entities must not be null");
		Assert.notNull(options, "WriteOptions must not be null");

		for (Object entity : entities) {

			Assert.notNull(entity, "Entity must not be null");
			assertNotStatement("delete", entity);
			assertNotQueryOptions(entity);

			CassandraPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(entity.getClass());

			SimpleStatement delete = getStatementFactory()
					.delete(entity, options, this.getConverter(), persistentEntity.getTableName()).build();

			addStatement(delete);
		}

		return this;
	}

	private void assertNotExecuted() {
		Assert.state(!this.executed.get(), "This Cassandra Batch was already executed");
	}

	private CassandraPersistentEntity<?> getRequiredPersistentEntity(Class<?> entityType) {
		return getMappingContext().getRequiredPersistentEntity(ClassUtils.getUserClass(entityType));
	}

	private static void assertNotQueryOptions(Object o) {

		if (o instanceof QueryOptions) {
			throw new IllegalArgumentException(
					String.format("%s must not be used as entity; Please make sure to call the appropriate method accepting %s",
							ClassUtils.getDescriptiveType(o), ClassUtils.getShortName(o.getClass())));
		}
	}

	private static void assertNotStatement(String operation, Object o) {

		if (o instanceof Statement<?>) {
			throw new IllegalArgumentException(String.format("%s cannot use a Statement: %s. Use only entities for %s",
					StringUtils.capitalize(operation), ClassUtils.getDescriptiveType(o), operation));
		}
	}
}

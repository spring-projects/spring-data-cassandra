/*
 * Copyright 2018-present the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
 * Default implementation for {@link ReactiveCassandraBatchOperations}.
 *
 * @author Oleh Dokuka
 * @author Mark Paluch
 * @author Sam Lightfoot
 * @since 2.1
 */
class ReactiveCassandraBatchTemplate implements ReactiveCassandraBatchOperations {

	private final AtomicBoolean executed = new AtomicBoolean();

	private final BatchStatementBuilder batch;

	private final CassandraConverter converter;

	private final CassandraMappingContext mappingContext;

	private final List<Mono<? extends Iterable<? extends BatchableStatement<?>>>> batchMonos = new CopyOnWriteArrayList<>();

	private final ReactiveCassandraOperations operations;

	private final StatementFactory statementFactory;

	private QueryOptions options = QueryOptions.empty();

	/**
	 * Create a new {@link CassandraBatchTemplate} given {@link CassandraOperations} and {@link BatchType}.
	 *
	 * @param operations must not be {@literal null}.
	 * @param batchType must not be {@literal null}.
	 * @since 3.2.6
	 */
	ReactiveCassandraBatchTemplate(ReactiveCassandraTemplate operations, BatchType batchType) {

		Assert.notNull(operations, "CassandraOperations must not be null");
		Assert.notNull(batchType, "BatchType must not be null");

		this.operations = operations;
		this.batch = BatchStatement.builder(batchType);
		this.converter = operations.getConverter();
		this.mappingContext = this.converter.getMappingContext();
		this.statementFactory = operations.getStatementFactory();
	}

	private void assertNotExecuted() {
		Assert.state(!this.executed.get(), "This Cassandra Batch was already executed");
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

	private CassandraPersistentEntity<?> getRequiredPersistentEntity(Class<?> entityType) {
		return getMappingContext().getRequiredPersistentEntity(ClassUtils.getUserClass(entityType));
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
	@SuppressWarnings("unchecked")
	public Mono<WriteResult> execute() {

		return Mono.defer(() -> {

			if (this.executed.compareAndSet(false, true)) {

				return Flux.merge(this.batchMonos) //
						.flatMap(Flux::fromIterable) //
						.collectList() //
						.flatMap(statements -> {

							this.batch.addStatements((List<BatchableStatement<?>>) statements);

							return this.operations.getReactiveCqlOperations()
									.queryForResultSet(QueryOptionsUtil.addQueryOptions(this.batch.build(), this.options));
						}) //
						.flatMap(resultSet -> resultSet.rows().collectList()
								.map(rows -> new WriteResult(resultSet.getAllExecutionInfo(), resultSet.wasApplied(), rows)));
			}

			return Mono.error(new IllegalStateException("This Cassandra Batch was already executed"));
		});
	}

	@Override
	public ReactiveCassandraBatchOperations withTimestamp(long timestamp) {

		assertNotExecuted();
		this.batch.setQueryTimestamp(timestamp);

		return this;
	}

	@Override
	public ReactiveCassandraBatchOperations withQueryOptions(QueryOptions options) {

		assertNotExecuted();
		Assert.notNull(options, "QueryOptions must not be null");

		this.options = options;

		return this;
	}

	@Override
	public ReactiveCassandraBatchOperations addStatement(Mono<? extends BatchableStatement<?>> statement) {

		assertNotExecuted();
		Assert.notNull(statement, "Statement mono must not be null");

		this.batchMonos.add(statement.map(List::of));

		return this;
	}

	@Override
	public ReactiveCassandraBatchOperations addStatements(
			Mono<? extends Iterable<? extends BatchableStatement<?>>> statements) {

		assertNotExecuted();
		Assert.notNull(statements, "Statements mono must not be null");

		this.batchMonos.add(statements);

		return this;
	}

	@Override
	public ReactiveCassandraBatchOperations insert(Object... entities) {

		Assert.notNull(entities, "Entities must not be null");

		return insert(Arrays.asList(entities));
	}

	@Override
	public ReactiveCassandraBatchOperations insert(Iterable<?> entities) {
		return insert(entities, InsertOptions.empty());
	}

	@Override
	public ReactiveCassandraBatchOperations insert(Mono<? extends Iterable<?>> entities) {
		return insert(entities, InsertOptions.empty());
	}

	@Override
	public ReactiveCassandraBatchOperations insert(Iterable<?> entities, WriteOptions options) {

		assertNotExecuted();
		Assert.notNull(entities, "Entities must not be null");
		Assert.notNull(options, "WriteOptions must not be null");

		addStatements(doInsert(entities, options));

		return this;
	}

	@Override
	public ReactiveCassandraBatchOperations insert(Mono<? extends Iterable<?>> entities, WriteOptions options) {

		assertNotExecuted();
		Assert.notNull(entities, "Entities must not be null");
		Assert.notNull(options, "WriteOptions must not be null");

		addStatements(entities.map(entity -> doInsert(entity, options)));

		return this;
	}

	private Collection<SimpleStatement> doInsert(Iterable<?> entities, WriteOptions options) {

		CassandraMappingContext mappingContext = getMappingContext();
		List<SimpleStatement> insertQueries = new ArrayList<>();

		for (Object entity : entities) {

			Assert.notNull(entity, "Entity must not be null");
			assertNotStatement("insert", entity);
			assertNotQueryOptions(entity);

			BasicCassandraPersistentEntity<?> persistentEntity = mappingContext
					.getRequiredPersistentEntity(entity.getClass());

			SimpleStatement insertQuery = getStatementFactory()
					.insert(entity, options, persistentEntity, persistentEntity.getTableName()).build();

			insertQueries.add(insertQuery);
		}

		return insertQueries;
	}

	@Override
	public ReactiveCassandraBatchOperations update(Object... entities) {

		Assert.notNull(entities, "Entities must not be null");

		return update(Arrays.asList(entities));
	}

	@Override
	public ReactiveCassandraBatchOperations update(Iterable<?> entities) {
		return update(entities, UpdateOptions.empty());
	}

	@Override
	public ReactiveCassandraBatchOperations update(Mono<? extends Iterable<?>> entities) {
		return update(entities, UpdateOptions.empty());
	}

	@Override
	public ReactiveCassandraBatchOperations update(Iterable<?> entities, WriteOptions options) {

		assertNotExecuted();
		Assert.notNull(entities, "Entities must not be null");
		Assert.notNull(options, "WriteOptions must not be null");

		addStatements(Mono.just(doUpdate(entities, options)));

		return this;
	}

	@Override
	public ReactiveCassandraBatchOperations update(Mono<? extends Iterable<?>> entities, WriteOptions options) {

		assertNotExecuted();
		Assert.notNull(entities, "Entities must not be null");
		Assert.notNull(options, "WriteOptions must not be null");

		addStatements(entities.map(entity -> doUpdate(entity, options)));

		return this;
	}

	private Collection<SimpleStatement> doUpdate(Iterable<?> entities, WriteOptions options) {

		List<SimpleStatement> updateQueries = new ArrayList<>();

		for (Object entity : entities) {

			Assert.notNull(entity, "Entity must not be null");
			assertNotStatement("update", entity);
			assertNotQueryOptions(entity);

			CassandraPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(entity.getClass());

			SimpleStatement update = getStatementFactory()
					.update(entity, options, persistentEntity, persistentEntity.getTableName()).build();

			updateQueries.add(update);
		}

		return updateQueries;
	}

	@Override
	public ReactiveCassandraBatchOperations delete(Object... entities) {

		Assert.notNull(entities, "Entities must not be null");

		return delete(Arrays.asList(entities));
	}

	@Override
	public ReactiveCassandraBatchOperations delete(Iterable<?> entities) {
		return delete(entities, DeleteOptions.empty());
	}

	@Override
	public ReactiveCassandraBatchOperations delete(Mono<? extends Iterable<?>> entities) {
		return delete(entities, DeleteOptions.empty());
	}

	@Override
	public ReactiveCassandraBatchOperations delete(Iterable<?> entities, WriteOptions options) {

		assertNotExecuted();
		Assert.notNull(entities, "Entities must not be null");
		Assert.notNull(options, "WriteOptions must not be null");

		addStatements(Mono.just(doDelete(entities, options)));

		return this;
	}

	@Override
	public ReactiveCassandraBatchOperations delete(Mono<? extends Iterable<?>> entities, WriteOptions options) {

		assertNotExecuted();
		Assert.notNull(entities, "Entities must not be null");
		Assert.notNull(options, "WriteOptions must not be null");

		addStatements(entities.map(it -> doDelete(it, options)));

		return this;
	}

	private Collection<SimpleStatement> doDelete(Iterable<?> entities, WriteOptions options) {

		List<SimpleStatement> deleteQueries = new ArrayList<>();

		for (Object entity : entities) {

			Assert.notNull(entity, "Entity must not be null");
			assertNotStatement("delete", entity);
			assertNotQueryOptions(entity);

			CassandraPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(entity.getClass());

			SimpleStatement delete = getStatementFactory()
					.delete(entity, options, getConverter(), persistentEntity.getTableName()).build();

			deleteQueries.add(delete);
		}

		return deleteQueries;
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

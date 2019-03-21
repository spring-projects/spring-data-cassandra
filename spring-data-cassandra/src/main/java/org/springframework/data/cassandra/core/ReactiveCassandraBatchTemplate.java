/*
 * Copyright 2018-2019 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.UpdateMapper;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

/**
 * Default implementation for {@link ReactiveCassandraBatchOperations}.
 *
 * @author Oleh Dokuka
 * @author Mark Paluch
 * @since 2.1
 */
class ReactiveCassandraBatchTemplate implements ReactiveCassandraBatchOperations {

	private final AtomicBoolean executed = new AtomicBoolean();

	private final Batch batch = QueryBuilder.batch();

	private final CassandraConverter converter;

	private final CassandraMappingContext mappingContext;

	private final List<Mono<Collection<? extends BuiltStatement>>> batchMonos = new CopyOnWriteArrayList<>();

	private final ReactiveCassandraOperations operations;

	private final StatementFactory statementFactory;

	/**
	 * Create a new {@link CassandraBatchTemplate} given {@link CassandraOperations}.
	 *
	 * @param operations must not be {@literal null}.
	 */
	ReactiveCassandraBatchTemplate(ReactiveCassandraOperations operations) {

		Assert.notNull(operations, "CassandraOperations must not be null");

		this.operations = operations;
		this.converter = operations.getConverter();
		this.mappingContext = this.converter.getMappingContext();
		this.statementFactory = new StatementFactory(new UpdateMapper(this.converter));
	}

	private void assertNotExecuted() {
		Assert.state(!this.executed.get(), "This Cassandra Batch was already executed");
	}

	/**
	 * Return a reference to the configured {@link CassandraConverter} used to map {@link Object Objects}
	 * to {@link com.datastax.driver.core.Row Rows}.
	 *
	 * @return a reference to the configured {@link CassandraConverter}.
	 * @see org.springframework.data.cassandra.core.convert.CassandraConverter
	 */
	protected CassandraConverter getConverter() {
		return this.converter;
	}

	/**
	 * Returns a reference to the configured {@link CassandraMappingContext} used to map entities to Cassandra tables
	 * and back.
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
	 * Return a reference to the configured {@link StatementFactory} used to create Cassandra {@link Statement} objects
	 * to perform data access operations on a Cassandra cluster.
	 *
	 * @return a reference to the configured {@link StatementFactory}.
	 * @see org.springframework.data.cassandra.core.StatementFactory
	 */
	protected StatementFactory getStatementFactory() {
		return this.statementFactory;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#execute()
	 */
	@Override
	public Mono<WriteResult> execute() {

		return Mono.defer(() -> {

			if (this.executed.compareAndSet(false, true)) {

				return Flux.merge(this.batchMonos) //
						.flatMapIterable(Function.identity()) //
						.collectList() //
						.flatMap(statements -> {

							statements.forEach(this.batch::add);

							return this.operations.getReactiveCqlOperations().queryForResultSet(this.batch);

						}).flatMap(resultSet -> resultSet.rows().collectList()
								.map(rows -> new WriteResult(resultSet.getAllExecutionInfo(), resultSet.wasApplied(), rows)));
			}

			return Mono.error(new IllegalStateException("This Cassandra Batch was already executed"));
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#withTimestamp(long)
	 */
	@Override
	public ReactiveCassandraBatchOperations withTimestamp(long timestamp) {

		assertNotExecuted();

		this.batch.using(QueryBuilder.timestamp(timestamp));

		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#insert(java.lang.Object[])
	 */
	@Override
	public ReactiveCassandraBatchOperations insert(Object... entities) {

		Assert.notNull(entities, "Entities must not be null");

		return insert(Arrays.asList(entities));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#insert(java.lang.Iterable)
	 */
	@Override
	public ReactiveCassandraBatchOperations insert(Iterable<?> entities) {
		return insert(entities, InsertOptions.empty());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#insert(reactor.core.publisher.Mono)
	 */
	@Override
	public ReactiveCassandraBatchOperations insert(Mono<? extends Iterable<?>> entities) {
		return insert(entities, InsertOptions.empty());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#insert(java.lang.Iterable, org.springframework.data.cassandra.core.cql.WriteOptions)
	 */
	@Override
	public ReactiveCassandraBatchOperations insert(Iterable<?> entities, WriteOptions options) {

		assertNotExecuted();

		Assert.notNull(entities, "Entities must not be null");
		Assert.notNull(options, "WriteOptions must not be null");

		this.batchMonos.add(Mono.just(doInsert(entities, options)));

		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#insert(reactor.core.publisher.Mono, org.springframework.data.cassandra.core.cql.WriteOptions)
	 */
	@Override
	public ReactiveCassandraBatchOperations insert(Mono<? extends Iterable<?>> entities, WriteOptions options) {

		assertNotExecuted();

		Assert.notNull(entities, "Entities must not be null");
		Assert.notNull(options, "WriteOptions must not be null");

		this.batchMonos.add(entities.map(entity -> doInsert(entity, options)));

		return this;
	}

	private Collection<? extends BuiltStatement> doInsert(Iterable<?> entities, WriteOptions options) {

		CassandraConverter converter = getConverter();
		CassandraMappingContext mappingContext = getMappingContext();
		List<Insert> insertQueries = new ArrayList<>();

		for (Object entity : entities) {

			Assert.notNull(entity, "Entity must not be null");

			BasicCassandraPersistentEntity<?> persistentEntity =
					mappingContext.getRequiredPersistentEntity(entity.getClass());

			Insert insertQuery = EntityQueryUtils.createInsertQuery(persistentEntity.getTableName().toCql(),
					entity, options, converter, persistentEntity);

			insertQueries.add(insertQuery);
		}

		return insertQueries;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#update(java.lang.Object[])
	 */
	@Override
	public ReactiveCassandraBatchOperations update(Object... entities) {

		Assert.notNull(entities, "Entities must not be null");

		return update(Arrays.asList(entities));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#update(java.lang.Iterable)
	 */
	@Override
	public ReactiveCassandraBatchOperations update(Iterable<?> entities) {
		return update(entities, UpdateOptions.empty());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#update(reactor.core.publisher.Mono)
	 */
	@Override
	public ReactiveCassandraBatchOperations update(Mono<? extends Iterable<?>> entities) {
		return update(entities, UpdateOptions.empty());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#update(java.lang.Iterable, org.springframework.data.cassandra.core.cql.WriteOptions)
	 */
	@Override
	public ReactiveCassandraBatchOperations update(Iterable<?> entities, WriteOptions options) {

		assertNotExecuted();

		Assert.notNull(entities, "Entities must not be null");
		Assert.notNull(options, "WriteOptions must not be null");

		this.batchMonos.add(Mono.just(doUpdate(entities, options)));

		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#update(reactor.core.publisher.Mono, org.springframework.data.cassandra.core.cql.WriteOptions)
	 */
	@Override
	public ReactiveCassandraBatchOperations update(Mono<? extends Iterable<?>> entities, WriteOptions options) {

		assertNotExecuted();

		Assert.notNull(entities, "Entities must not be null");
		Assert.notNull(options, "WriteOptions must not be null");

		this.batchMonos.add(entities.map(entity -> doUpdate(entity, options)));

		return this;
	}

	private Collection<? extends BuiltStatement> doUpdate(Iterable<?> entities, WriteOptions options) {

		CassandraConverter converter = getConverter();
		List<Update> updateQueries = new ArrayList<>();

		for (Object entity : entities) {

			Assert.notNull(entity, "Entity must not be null");

			CassandraPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(entity.getClass());

			Update update = getStatementFactory()
					.update(entity, options, converter, persistentEntity, persistentEntity.getTableName());

			updateQueries.add(update);
		}

		return updateQueries;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#delete(java.lang.Object[])
	 */
	@Override
	public ReactiveCassandraBatchOperations delete(Object... entities) {

		Assert.notNull(entities, "Entities must not be null");

		return delete(Arrays.asList(entities));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#delete(java.lang.Iterable)
	 */
	@Override
	public ReactiveCassandraBatchOperations delete(Iterable<?> entities) {
		return delete(entities, DeleteOptions.empty());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#delete(reactor.core.publisher.Mono)
	 */
	@Override
	public ReactiveCassandraBatchOperations delete(Mono<? extends Iterable<?>> entities) {
		return delete(entities, DeleteOptions.empty());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#delete(java.lang.Iterable, org.springframework.data.cassandra.core.cql.WriteOptions)
	 */
	@Override
	public ReactiveCassandraBatchOperations delete(Iterable<?> entities, WriteOptions options) {

		assertNotExecuted();

		Assert.notNull(entities, "Entities must not be null");
		Assert.notNull(options, "WriteOptions must not be null");

		this.batchMonos.add(Mono.just(doDelete(entities, options)));

		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#delete(reactor.core.publisher.Mono, org.springframework.data.cassandra.core.cql.WriteOptions)
	 */
	@Override
	public ReactiveCassandraBatchOperations delete(Mono<? extends Iterable<?>> entities, WriteOptions options) {

		assertNotExecuted();

		Assert.notNull(entities, "Entities must not be null");
		Assert.notNull(options, "WriteOptions must not be null");

		this.batchMonos.add(entities.map(it -> doDelete(it, options)));

		return this;
	}

	private Collection<? extends BuiltStatement> doDelete(Iterable<?> entities, WriteOptions options) {

		CassandraConverter converter = getConverter();
		List<Delete> deleteQueries = new ArrayList<>();

		for (Object entity : entities) {

			Assert.notNull(entity, "Entity must not be null");

			CassandraPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(entity.getClass());

			Delete delete = getStatementFactory()
					.delete(entity, options, converter, persistentEntity, persistentEntity.getTableName());

			deleteQueries.add(delete);
		}

		return deleteQueries;
	}
}

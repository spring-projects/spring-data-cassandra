/*
 * Copyright 2018-2019 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

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

	private final ReactiveCassandraOperations operations;

	private final AtomicBoolean executed = new AtomicBoolean();

	private final Batch batch = QueryBuilder.batch();

	private final List<Mono<Collection<? extends BuiltStatement>>> batchMonos = new CopyOnWriteArrayList<>();

	/**
	 * Create a new {@link CassandraBatchTemplate} given {@link CassandraOperations}.
	 *
	 * @param operations must not be {@literal null}.
	 */
	ReactiveCassandraBatchTemplate(ReactiveCassandraOperations operations) {

		Assert.notNull(operations, "CassandraOperations must not be null");

		this.operations = operations;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#execute()
	 */
	@Override
	public Mono<WriteResult> execute() {

		return Mono.defer(() -> {

			if (executed.compareAndSet(false, true)) {

				return Flux.merge(batchMonos) //
						.flatMapIterable(Function.identity()) //
						.collectList() //
						.flatMap(statements -> {

							statements.forEach(batch::add);

							return operations.getReactiveCqlOperations().queryForResultSet(batch);
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

		batch.using(QueryBuilder.timestamp(timestamp));

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

		batchMonos.add(Mono.just(doInsert(entities, options)));

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

		batchMonos.add(entities.map(e -> doInsert(e, options)));

		return this;
	}

	private Collection<? extends BuiltStatement> doInsert(Iterable<?> entities, WriteOptions options) {

		List<Insert> insertQueries = new ArrayList<>();
		CassandraConverter converter = operations.getConverter();
		CassandraMappingContext mappingContext = converter.getMappingContext();

		for (Object entity : entities) {

			Assert.notNull(entity, "Entity must not be null");

			BasicCassandraPersistentEntity<?> persistentEntity = mappingContext
					.getRequiredPersistentEntity(entity.getClass());
			insertQueries.add(QueryUtils.createInsertQuery(persistentEntity.getTableName().toCql(), entity, options,
					converter, persistentEntity));
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

		batchMonos.add(Mono.just(doUpdate(entities, options)));

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

		batchMonos.add(entities.map(e -> doUpdate(e, options)));

		return this;
	}

	private Collection<? extends BuiltStatement> doUpdate(Iterable<?> entities, WriteOptions options) {

		List<Update> updateQueries = new ArrayList<>();
		CassandraConverter converter = operations.getConverter();

		for (Object entity : entities) {

			Assert.notNull(entity, "Entity must not be null");

			updateQueries.add(QueryUtils.createUpdateQuery(getTable(entity), entity, options, converter));
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

		assertNotExecuted();
		Assert.notNull(entities, "Entities must not be null");

		batchMonos.add(Mono.just(doDelete(entities)));

		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraBatchOperations#delete(reactor.core.publisher.Mono)
	 */
	@Override
	public ReactiveCassandraBatchOperations delete(Mono<? extends Iterable<?>> entities) {

		assertNotExecuted();
		Assert.notNull(entities, "Entities must not be null");

		batchMonos.add(entities.map(this::doDelete));

		return this;
	}

	private Collection<? extends BuiltStatement> doDelete(Iterable<?> entities) {

		List<Delete> deleteQueries = new ArrayList<>();
		CassandraConverter converter = operations.getConverter();

		for (Object entity : entities) {

			Assert.notNull(entity, "Entity must not be null");

			deleteQueries.add(QueryUtils.createDeleteQuery(getTable(entity), entity, QueryOptions.empty(), converter));
		}

		return deleteQueries;
	}

	private void assertNotExecuted() {
		Assert.state(!executed.get(), "This Cassandra Batch was already executed");
	}

	private String getTable(Object entity) {

		Assert.notNull(entity, "Entity must not be null");

		return operations.getConverter().getMappingContext()
				.getRequiredPersistentEntity(ClassUtils.getUserClass(entity.getClass())).getTableName().toCql();
	}
}

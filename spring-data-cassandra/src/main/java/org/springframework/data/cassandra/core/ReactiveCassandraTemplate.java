/*
 * Copyright 2016-2017 the original author or authors.
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

import org.reactivestreams.Publisher;
import org.springframework.cassandra.core.CqlProvider;
import org.springframework.cassandra.core.QueryOptions;
import org.springframework.cassandra.core.ReactiveCqlOperations;
import org.springframework.cassandra.core.ReactiveCqlTemplate;
import org.springframework.cassandra.core.ReactiveSessionCallback;
import org.springframework.cassandra.core.WriteOptions;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.cassandra.core.session.DefaultReactiveSessionFactory;
import org.springframework.cassandra.core.session.ReactiveResultSet;
import org.springframework.cassandra.core.session.ReactiveSession;
import org.springframework.cassandra.core.session.ReactiveSessionFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.convert.QueryMapper;
import org.springframework.data.cassandra.convert.UpdateMapper;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Truncate;
import com.datastax.driver.core.querybuilder.Update;

/**
 * Primary implementation of {@link ReactiveCassandraOperations}. It simplifies the use of Reactive Cassandra usage and
 * helps to avoid common errors. It executes core Cassandra workflow. This class executes CQL queries or updates,
 * initiating iteration over {@link ReactiveResultSet} and catching Cassandra exceptions and translating them to the
 * generic, more informative exception hierarchy defined in the {@code org.springframework.dao} package.
 * <p>
 * Can be used within a service implementation via direct instantiation with a {@link ReactiveSessionFactory} reference,
 * or get prepared in an application context and given to services as bean reference.
 * <p>
 * Note: The {@link ReactiveSessionFactory} should always be configured as a bean in the application context, in the
 * first case given to the service directly, in the second case to the prepared template.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class ReactiveCassandraTemplate implements ReactiveCassandraOperations {

	private final CassandraConverter converter;

	private final CassandraMappingContext mappingContext;

	private final ReactiveCqlOperations cqlOperations;

	private final StatementFactory statementFactory;

	/**
	 * Creates an instance of {@link ReactiveCassandraTemplate} initialized with the given {@link ReactiveSession} and a
	 * default {@link MappingCassandraConverter}.
	 *
	 * @param session {@link ReactiveSession} used to interact with Cassandra; must not be {@literal null}.
	 * @see CassandraConverter
	 * @see Session
	 */
	public ReactiveCassandraTemplate(ReactiveSession session) {
		this(session, newConverter());
	}

	/**
	 * Create an instance of {@link CassandraTemplate} initialized with the given {@link ReactiveSession} and
	 * {@link CassandraConverter}.
	 *
	 * @param session {@link ReactiveSession} used to interact with Cassandra; must not be {@literal null}.
	 * @param converter {@link CassandraConverter} used to convert between Java and Cassandra types; must not be
	 *          {@literal null}.
	 * @see org.springframework.data.cassandra.convert.CassandraConverter
	 * @see com.datastax.driver.core.Session
	 */
	public ReactiveCassandraTemplate(ReactiveSession session, CassandraConverter converter) {
		this(new DefaultReactiveSessionFactory(session), converter);
	}

	/**
	 * Create an instance of {@link ReactiveCassandraTemplate} initialized with the given {@link ReactiveSessionFactory}
	 * and {@link CassandraConverter}.
	 *
	 * @param sessionFactory {@link ReactiveSessionFactory} used to interact with Cassandra; must not be {@literal null}.
	 * @param converter {@link CassandraConverter} used to convert between Java and Cassandra types; must not be
	 *          {@literal null}.
	 * @see org.springframework.data.cassandra.convert.CassandraConverter
	 * @see com.datastax.driver.core.Session
	 */
	public ReactiveCassandraTemplate(ReactiveSessionFactory sessionFactory, CassandraConverter converter) {

		Assert.notNull(sessionFactory, "ReactiveSessionFactory must not be null");
		Assert.notNull(converter, "CassandraConverter must not be null");

		this.converter = converter;
		this.mappingContext = this.converter.getMappingContext();
		this.cqlOperations = new ReactiveCqlTemplate(sessionFactory);
		this.statementFactory = new StatementFactory(new QueryMapper(converter), new UpdateMapper(converter));
	}

	/**
	 * Create an instance of {@link ReactiveCassandraTemplate} initialized with the given {@link ReactiveCqlOperations}
	 * and {@link CassandraConverter}.
	 *
	 * @param reactiveCqlOperations {@link ReactiveCqlOperations} used to interact with Cassandra; must not be
	 *          {@literal null}.
	 * @param converter {@link CassandraConverter} used to convert between Java and Cassandra types; must not be
	 *          {@literal null}.
	 * @see org.springframework.data.cassandra.convert.CassandraConverter
	 * @see com.datastax.driver.core.Session
	 */
	public ReactiveCassandraTemplate(ReactiveCqlOperations reactiveCqlOperations, CassandraConverter converter) {

		Assert.notNull(reactiveCqlOperations, "ReactiveCqlOperations must not be null");
		Assert.notNull(converter, "CassandraConverter must not be null");

		this.converter = converter;
		this.mappingContext = this.converter.getMappingContext();
		this.cqlOperations = reactiveCqlOperations;
		this.statementFactory = new StatementFactory(new QueryMapper(converter), new UpdateMapper(converter));
	}

	private static MappingCassandraConverter newConverter() {

		MappingCassandraConverter converter = new MappingCassandraConverter();

		converter.afterPropertiesSet();

		return converter;
	}

	// -------------------------------------------------------------------------
	// Methods dealing with static CQL
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#select(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> Flux<T> select(String cql, Class<T> entityClass) {

		Assert.hasText(cql, "Statement must not be empty");

		return select(new SimpleStatement(cql), entityClass);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#selectOne(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> Mono<T> selectOne(String cql, Class<T> entityClass) {
		return select(cql, entityClass).next();
	}

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.driver.core.Statement
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#select(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> Flux<T> select(Statement cql, Class<T> entityClass) {

		Assert.notNull(cql, "Statement must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return cqlOperations.query(cql, (row, rowNum) -> converter.read(entityClass, row));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#selectOne(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> Mono<T> selectOne(Statement statement, Class<T> entityClass) {
		return select(statement, entityClass).next();
	}

	// -------------------------------------------------------------------------
	// Methods dealing with org.springframework.data.cassandra.core.query.Query
	// -------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#select(org.springframework.data.cassandra.core.query.Query, java.lang.Class)
	 */
	@Override
	public <T> Flux<T> select(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return select(statementFactory.select(query, mappingContext.getRequiredPersistentEntity(entityClass)), entityClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#selectOne(org.springframework.data.cassandra.core.query.Query, java.lang.Class)
	 */
	@Override
	public <T> Mono<T> selectOne(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return selectOne(statementFactory.select(query, mappingContext.getRequiredPersistentEntity(entityClass)),
				entityClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#update(org.springframework.data.cassandra.core.query.Query, org.springframework.data.cassandra.core.query.Update, java.lang.Class)
	 */
	@Override
	public Mono<Boolean> update(Query query, org.springframework.data.cassandra.core.query.Update update,
			Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(update, "Update must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return cqlOperations
				.execute(statementFactory.update(query, update, mappingContext.getRequiredPersistentEntity(entityClass)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#delete(org.springframework.data.cassandra.core.query.Query, java.lang.Class)
	 */
	@Override
	public Mono<Boolean> delete(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return cqlOperations
				.execute(statementFactory.delete(query, mappingContext.getRequiredPersistentEntity(entityClass)));
	}

	// -------------------------------------------------------------------------
	// Methods dealing with entities
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#selectOneById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> Mono<T> selectOneById(Object id, Class<T> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);

		Select select = QueryBuilder.select().all().from(entity.getTableName().toCql());

		converter.write(id, select.where(), entity);

		return selectOne(select, entityClass);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#exists(java.lang.Object, java.lang.Class)
	 */
	@Override
	public Mono<Boolean> exists(Object id, Class<?> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);

		Select select = QueryBuilder.select().from(entity.getTableName().toCql());

		converter.write(id, select.where(), entity);

		return cqlOperations.queryForRows(select).hasElements();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#count(java.lang.Class)
	 */
	@Override
	public Mono<Long> count(Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");

		Select select = QueryBuilder.select().countAll()
				.from(mappingContext.getRequiredPersistentEntity(entityClass).getTableName().toCql());

		return cqlOperations.queryForObject(select, Long.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#insert(java.lang.Object)
	 */
	@Override
	public <T> Mono<T> insert(T entity) {
		return insert(entity, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#insert(java.lang.Object, org.springframework.cassandra.core.WriteOptions)
	 */
	@Override
	public <T> Mono<T> insert(T entity, WriteOptions options) {

		Assert.notNull(entity, "Entity must not be null");

		Insert insert = QueryUtils.createInsertQuery(getTableName(entity).toCql(), entity, options, converter);

		class InsertCallback implements ReactiveSessionCallback<T>, CqlProvider {

			@Override
			public Publisher<T> doInSession(ReactiveSession session) throws DriverException, DataAccessException {
				return session.execute(insert)
						.flatMap(reactiveResultSet -> reactiveResultSet.wasApplied() ? Mono.just(entity) : Mono.empty());
			}

			@Override
			public String getCql() {
				return insert.toString();
			}
		}

		return cqlOperations.execute(new InsertCallback()).next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#insert(org.reactivestreams.Publisher)
	 */
	@Override
	public <T> Flux<T> insert(Publisher<? extends T> entities) {
		return insert(entities, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#insert(org.reactivestreams.Publisher, org.springframework.cassandra.core.WriteOptions)
	 */
	@Override
	public <T> Flux<T> insert(Publisher<? extends T> entities, WriteOptions options) {

		Assert.notNull(entities, "Entity publisher must not be null");

		return Flux.from(entities).flatMap(entity -> insert(entity, options));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#update(java.lang.Object)
	 */
	@Override
	public <T> Mono<T> update(T entity) {
		return update(entity, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#update(java.lang.Object, org.springframework.cassandra.core.WriteOptions)
	 */
	@Override
	public <T> Mono<T> update(T entity, WriteOptions options) {

		Assert.notNull(entity, "Entity must not be null");

		Update update = QueryUtils.createUpdateQuery(getTableName(entity).toCql(), entity, options, converter);

		class UpdateCallback implements ReactiveSessionCallback<T>, CqlProvider {

			@Override
			public Publisher<T> doInSession(ReactiveSession session) throws DriverException, DataAccessException {
				return session.execute(update)
						.flatMap(reactiveResultSet -> reactiveResultSet.wasApplied() ? Mono.just(entity) : Mono.empty());
			}

			@Override
			public String getCql() {
				return update.toString();
			}
		}

		return cqlOperations.execute(new UpdateCallback()).next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#update(org.reactivestreams.Publisher)
	 */
	@Override
	public <T> Flux<T> update(Publisher<? extends T> entities) {
		return update(entities, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#update(org.reactivestreams.Publisher, org.springframework.cassandra.core.WriteOptions)
	 */
	@Override
	public <T> Flux<T> update(Publisher<? extends T> entities, WriteOptions options) {

		Assert.notNull(entities, "Entity publisher must not be null");

		return Flux.from(entities).flatMap(entity -> update(entity, options));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#deleteById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public Mono<Boolean> deleteById(Object id, Class<?> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);

		Delete delete = QueryBuilder.delete().from(entity.getTableName().toCql());

		converter.write(id, delete.where(), entity);

		return cqlOperations.execute(delete);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#delete(java.lang.Object)
	 */
	@Override
	public <T> Mono<T> delete(T entity) {
		return delete(entity, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#delete(java.lang.Object, org.springframework.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> Mono<T> delete(T entity, QueryOptions options) {

		Assert.notNull(entity, "Entity must not be null");

		Delete delete = QueryUtils.createDeleteQuery(getTableName(entity).toCql(), entity, options, converter);

		class DeleteCallback implements ReactiveSessionCallback<T>, CqlProvider {

			@Override
			public Publisher<T> doInSession(ReactiveSession session) throws DriverException, DataAccessException {
				return session.execute(delete)
						.flatMap(reactiveResultSet -> reactiveResultSet.wasApplied() ? Mono.just(entity) : Mono.empty());
			}

			@Override
			public String getCql() {
				return delete.toString();
			}
		}

		return cqlOperations.execute(new DeleteCallback()).next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#delete(org.reactivestreams.Publisher)
	 */
	@Override
	public <T> Flux<T> delete(Publisher<? extends T> entities) {
		return delete(entities, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#delete(org.reactivestreams.Publisher, org.springframework.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> Flux<T> delete(Publisher<? extends T> entities, QueryOptions options) {

		Assert.notNull(entities, "Entity publisher must not be null");

		return Flux.from(entities).flatMap(entity -> delete(entity, options));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#truncate(java.lang.Class)
	 */
	@Override
	public Mono<Void> truncate(Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");

		Truncate truncate = QueryBuilder
				.truncate(mappingContext.getRequiredPersistentEntity(entityClass).getTableName().toCql());

		return cqlOperations.execute(truncate).then();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#getConverter()
	 */
	@Override
	public CassandraConverter getConverter() {
		return converter;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#getReactiveCqlOperations()
	 */
	@Override
	public ReactiveCqlOperations getReactiveCqlOperations() {
		return cqlOperations;
	}

	private CqlIdentifier getTableName(Object entity) {
		return mappingContext.getRequiredPersistentEntity(ClassUtils.getUserClass(entity)).getTableName();
	}
}

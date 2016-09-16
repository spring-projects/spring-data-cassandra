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

import static org.springframework.data.cassandra.core.CassandraTemplate.*;

import org.reactivestreams.Publisher;
import org.springframework.cassandra.core.DefaultReactiveSessionFactory;
import org.springframework.cassandra.core.QueryOptions;
import org.springframework.cassandra.core.ReactiveCqlOperations;
import org.springframework.cassandra.core.ReactiveCqlTemplate;
import org.springframework.cassandra.core.ReactiveResultSet;
import org.springframework.cassandra.core.ReactiveSession;
import org.springframework.cassandra.core.ReactiveSessionCallback;
import org.springframework.cassandra.core.ReactiveSessionFactory;
import org.springframework.cassandra.core.WriteOptions;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Truncate;
import com.datastax.driver.core.querybuilder.Update;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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

		CassandraPersistentEntity<?> entity = getPersistentEntity(entityClass);
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

		CassandraPersistentEntity<?> entity = getPersistentEntity(entityClass);
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

		Select select = QueryBuilder.select().countAll().from(getPersistentEntity(entityClass).getTableName().toCql());

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

		CqlIdentifier tableName = getTableName(entity);

		Insert insertQuery = createInsertQuery(tableName.toCql(), entity, options, converter);

		return cqlOperations.execute((ReactiveSessionCallback<T>) session -> (Publisher<T>) session.execute(insertQuery)
				.flatMap(reactiveResultSet -> reactiveResultSet.wasApplied() ? Mono.just(entity) : Mono.empty())).next();
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

		CqlIdentifier tableName = getTableName(entity);

		Update update = createUpdateQuery(tableName.toCql(), entity, options, converter);

		return cqlOperations.execute((ReactiveSessionCallback<T>) session -> (Publisher<T>) session.execute(update)
				.flatMap(reactiveResultSet -> reactiveResultSet.wasApplied() ? Mono.just(entity) : Mono.empty())).next();
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

		CassandraPersistentEntity<?> entity = getPersistentEntity(entityClass);
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

		CqlIdentifier tableName = getTableName(entity);

		Delete delete = createDeleteQuery(tableName.toCql(), entity, options, converter);

		return cqlOperations.execute((ReactiveSessionCallback<T>) session -> (Publisher<T>) session.execute(delete)
				.flatMap(reactiveResultSet -> reactiveResultSet.wasApplied() ? Mono.just(entity) : Mono.empty())).next();
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
		Truncate truncate = QueryBuilder.truncate(getPersistentEntity(entityClass).getTableName().toCql());

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

	private <T> CassandraPersistentEntity<?> getPersistentEntity(Class<T> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		if (entity == null) {
			throw new InvalidDataAccessApiUsageException(
					String.format("No Persistent Entity information found for the class [%s]", entityClass.getName()));
		}

		return entity;
	}

	private CqlIdentifier getTableName(Object entity) {
		return getPersistentEntity(ClassUtils.getUserClass(entity)).getTableName();
	}
}

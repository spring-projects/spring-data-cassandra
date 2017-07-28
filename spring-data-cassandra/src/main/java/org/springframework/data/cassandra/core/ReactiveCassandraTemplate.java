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

import lombok.NonNull;
import lombok.Value;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.reactivestreams.Publisher;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.data.cassandra.ReactiveSessionFactory;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.convert.QueryMapper;
import org.springframework.data.cassandra.core.convert.UpdateMapper;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.cql.CqlProvider;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.ReactiveCqlOperations;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.data.cassandra.core.cql.ReactiveSessionCallback;
import org.springframework.data.cassandra.core.cql.session.DefaultReactiveSessionFactory;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.mapping.context.MappingContext;
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
 * @author John Blum
 * @since 2.0
 */
public class ReactiveCassandraTemplate implements ReactiveCassandraOperations {

	private final CassandraConverter converter;

	private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

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
	 * @see org.springframework.data.cassandra.core.convert.CassandraConverter
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
	 * @see org.springframework.data.cassandra.core.convert.CassandraConverter
	 * @see com.datastax.driver.core.Session
	 */
	public ReactiveCassandraTemplate(ReactiveSessionFactory sessionFactory, CassandraConverter converter) {

		Assert.notNull(sessionFactory, "ReactiveSessionFactory must not be null");
		Assert.notNull(converter, "CassandraConverter must not be null");

		this.converter = converter;
		this.cqlOperations = new ReactiveCqlTemplate(sessionFactory);
		this.mappingContext = this.converter.getMappingContext();
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
	 * @see org.springframework.data.cassandra.core.convert.CassandraConverter
	 * @see com.datastax.driver.core.Session
	 */
	public ReactiveCassandraTemplate(ReactiveCqlOperations reactiveCqlOperations, CassandraConverter converter) {

		Assert.notNull(reactiveCqlOperations, "ReactiveCqlOperations must not be null");
		Assert.notNull(converter, "CassandraConverter must not be null");

		this.converter = converter;
		this.cqlOperations = reactiveCqlOperations;
		this.mappingContext = this.converter.getMappingContext();
		this.statementFactory = new StatementFactory(new QueryMapper(converter), new UpdateMapper(converter));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#getConverter()
	 */
	@Override
	public CassandraConverter getConverter() {
		return this.converter;
	}

	/* (non-Javadoc) */
	private static MappingCassandraConverter newConverter() {

		MappingCassandraConverter converter = new MappingCassandraConverter();

		converter.afterPropertiesSet();

		return converter;
	}

	/**
	 * Returns the {@link CassandraMappingContext} used by this template to access mapping meta-data used to store (map)
	 * objects to Cassandra tables.
	 *
	 * @return the {@link CassandraMappingContext} used by this template.
	 * @see CassandraMappingContext
	 */
	protected MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> getMappingContext() {
		return this.mappingContext;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#getReactiveCqlOperations()
	 */
	@Override
	public ReactiveCqlOperations getReactiveCqlOperations() {
		return this.cqlOperations;
	}

	/**
	 * Returns the {@link StatementFactory} used by this template to construct and run Cassandra CQL statements.
	 *
	 * @return the {@link StatementFactory} used by this template to construct and run Cassandra CQL statements.
	 * @see org.springframework.data.cassandra.core.StatementFactory
	 */
	protected StatementFactory getStatementFactory() {
		return this.statementFactory;
	}

	/* (non-Javadoc) */
	private CqlIdentifier getTableName(Object entity) {
		return getMappingContext().getRequiredPersistentEntity(ClassUtils.getUserClass(entity)).getTableName();
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

		return getReactiveCqlOperations().query(cql, (row, rowNum) -> getConverter().read(entityClass, row));
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

		return select(getStatementFactory().select(query,
				getMappingContext().getRequiredPersistentEntity(entityClass)), entityClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#selectOne(org.springframework.data.cassandra.core.query.Query, java.lang.Class)
	 */
	@Override
	public <T> Mono<T> selectOne(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return selectOne(getStatementFactory().select(query,
				getMappingContext().getRequiredPersistentEntity(entityClass)), entityClass);
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

		return getReactiveCqlOperations().execute(getStatementFactory().update(query, update,
				getMappingContext().getRequiredPersistentEntity(entityClass)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#delete(org.springframework.data.cassandra.core.query.Query, java.lang.Class)
	 */
	@Override
	public Mono<Boolean> delete(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return getReactiveCqlOperations().execute(getStatementFactory().delete(query,
				getMappingContext().getRequiredPersistentEntity(entityClass)));
	}

	// -------------------------------------------------------------------------
	// Methods dealing with entities
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#count(java.lang.Class)
	 */
	@Override
	public Mono<Long> count(Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");

		Select select = QueryBuilder.select().countAll()
			.from(getMappingContext().getRequiredPersistentEntity(entityClass).getTableName().toCql());

		return getReactiveCqlOperations().queryForObject(select, Long.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#exists(java.lang.Object, java.lang.Class)
	 */
	@Override
	public Mono<Boolean> exists(Object id, Class<?> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getMappingContext().getRequiredPersistentEntity(entityClass);

		Select select = QueryBuilder.select().from(entity.getTableName().toCql());

		getConverter().write(id, select.where(), entity);

		return getReactiveCqlOperations().queryForRows(select).hasElements();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#selectOneById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> Mono<T> selectOneById(Object id, Class<T> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getMappingContext().getRequiredPersistentEntity(entityClass);

		Select select = QueryBuilder.select().all().from(entity.getTableName().toCql());

		getConverter().write(id, select.where(), entity);

		return selectOne(select, entityClass);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#insert(java.lang.Object)
	 */
	@Override
	public <T> Mono<T> insert(T entity) {
		return insert(entity, InsertOptions.empty()).map(writeResult -> entity);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#insert(java.lang.Object, org.springframework.data.cassandra.core.InsertOptions)
	 */
	@Override
	public Mono<WriteResult> insert(Object entity, InsertOptions options) {

		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(options, "InsertOptions must not be null");

		Insert insert = QueryUtils.createInsertQuery(getTableName(entity).toCql(), entity, options, getConverter());

		return getReactiveCqlOperations().execute(new StatementCallback(insert)).next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#update(java.lang.Object)
	 */
	@Override
	public <T> Mono<T> update(T entity) {
		return update(entity, UpdateOptions.empty()).map(writeResult -> entity);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#update(java.lang.Object, org.springframework.data.cassandra.core.UpdateOptions)
	 */
	@Override
	public Mono<WriteResult> update(Object entity, UpdateOptions options) {

		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(options, "UpdateOptions must not be null");

		Update update = QueryUtils.createUpdateQuery(getTableName(entity).toCql(), entity, options, converter);

		return getReactiveCqlOperations().execute(new StatementCallback(update)).next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#delete(java.lang.Object)
	 */
	@Override
	public <T> Mono<T> delete(T entity) {
		return delete(entity, QueryOptions.empty()).map(reactiveWriteResult -> entity);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#delete(java.lang.Object, org.springframework.data.cql.core.QueryOptions)
	 */
	@Override
	public Mono<WriteResult> delete(Object entity, QueryOptions options) {

		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(options, "QueryOptions must not be null");

		Delete delete = QueryUtils.createDeleteQuery(getTableName(entity).toCql(), entity, options, getConverter());

		return getReactiveCqlOperations().execute(new StatementCallback(delete)).next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#deleteById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public Mono<Boolean> deleteById(Object id, Class<?> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getMappingContext().getRequiredPersistentEntity(entityClass);

		Delete delete = QueryBuilder.delete().from(entity.getTableName().toCql());

		getConverter().write(id, delete.where(), entity);

		return getReactiveCqlOperations().execute(delete);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveCassandraOperations#truncate(java.lang.Class)
	 */
	@Override
	public Mono<Void> truncate(Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");

		Truncate truncate = QueryBuilder
				.truncate(getMappingContext().getRequiredPersistentEntity(entityClass).getTableName().toCql());

		return getReactiveCqlOperations().execute(truncate).then();
	}

	@Value
	static class StatementCallback implements ReactiveSessionCallback<WriteResult>, CqlProvider {

		@NonNull Statement statement;

		@Override
		public Publisher<WriteResult> doInSession(ReactiveSession session) throws DriverException, DataAccessException {
			return session.execute(statement).flatMap(StatementCallback::toWriteResult);
		}

		@Override
		public String getCql() {
			return statement.toString();
		}

		private static Mono<WriteResult> toWriteResult(ReactiveResultSet resultSet) {
			return resultSet.rows().collectList()
					.map(rows -> new WriteResult(resultSet.getAllExecutionInfo(), resultSet.wasApplied(), rows));
		}
	}
}

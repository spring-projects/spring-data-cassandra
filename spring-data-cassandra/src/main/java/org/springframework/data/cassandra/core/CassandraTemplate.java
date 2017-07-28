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

import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.convert.QueryMapper;
import org.springframework.data.cassandra.core.convert.UpdateMapper;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.cql.CqlOperations;
import org.springframework.data.cassandra.core.cql.CqlProvider;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.SessionCallback;
import org.springframework.data.cassandra.core.cql.session.DefaultSessionFactory;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.datastax.driver.core.ResultSet;
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
 * Primary implementation of {@link CassandraOperations}. It simplifies the use of Cassandra usage and helps to avoid
 * common errors. It executes core Cassandra workflow. This class executes CQL queries or updates, initiating iteration
 * over {@link ResultSet} and catching Cassandra exceptions and translating them to the generic, more informative
 * exception hierarchy defined in the {@code org.springframework.dao} package.
 * <p>
 * Can be used within a service implementation via direct instantiation with a {@link Session} reference, or get
 * prepared in an application context and given to services as bean reference.
 * <p>
 * Note: The {@link Session} should always be configured as a bean in the application context, in the first case given
 * to the service directly, in the second case to the prepared template.
 *
 * @author Mark Paluch
 * @author John Blum
 * @since 2.0
 */
public class CassandraTemplate implements CassandraOperations {

	private final CassandraConverter converter;

	private final CqlOperations cqlOperations;

	private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	private final StatementFactory statementFactory;

	/**
	 * Creates an instance of {@link CassandraTemplate} initialized with the given {@link Session} and a default
	 * {@link MappingCassandraConverter}.
	 *
	 * @param session {@link Session} used to interact with Cassandra; must not be {@literal null}.
	 * @see CassandraConverter
	 * @see Session
	 */
	public CassandraTemplate(Session session) {
		this(session, newConverter());
	}

	/**
	 * Creates an instance of {@link CassandraTemplate} initialized with the given {@link Session} and
	 * {@link CassandraConverter}.
	 *
	 * @param session {@link Session} used to interact with Cassandra; must not be {@literal null}.
	 * @param converter {@link CassandraConverter} used to convert between Java and Cassandra types; must not be
	 *          {@literal null}.
	 * @see CassandraConverter
	 * @see Session
	 */
	public CassandraTemplate(Session session, CassandraConverter converter) {
		this(new DefaultSessionFactory(session), converter);
	}

	/**
	 * Creates an instance of {@link CassandraTemplate} initialized with the given {@link SessionFactory} and
	 * {@link CassandraConverter}.
	 *
	 * @param sessionFactory {@link SessionFactory} used to interact with Cassandra; must not be {@literal null}.
	 * @param converter {@link CassandraConverter} used to convert between Java and Cassandra types; must not be
	 *          {@literal null}.
	 * @see CassandraConverter
	 * @see SessionFactory
	 */
	public CassandraTemplate(SessionFactory sessionFactory, CassandraConverter converter) {
		this(new CqlTemplate(sessionFactory), converter);
	}

	/**
	 * Creates an instance of {@link CassandraTemplate} initialized with the given {@link CqlOperations} and
	 * {@link CassandraConverter}.
	 *
	 * @param cqlOperations {@link CqlOperations} used to interact with Cassandra; must not be {@literal null}.
	 * @param converter {@link CassandraConverter} used to convert between Java and Cassandra types; must not be
	 *          {@literal null}.
	 * @see CassandraConverter
	 * @see Session
	 */
	public CassandraTemplate(CqlOperations cqlOperations, CassandraConverter converter) {

		Assert.notNull(cqlOperations, "CqlOperations must not be null");
		Assert.notNull(converter, "CassandraConverter must not be null");

		this.converter = converter;
		this.cqlOperations = cqlOperations;
		this.mappingContext = converter.getMappingContext();
		this.statementFactory = new StatementFactory(new QueryMapper(converter), new UpdateMapper(converter));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#getConverter()
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#CqlOperations()
	 */
	@Override
	public CqlOperations getCqlOperations() {
		return this.cqlOperations;
	}

	/**
	 * Returns the {@link CassandraMappingContext} used by this template to access mapping meta-data used to store (map)
	 * object to Cassandra tables.
	 *
	 * @return the {@link CassandraMappingContext} used by this template.
	 * @see CassandraMappingContext
	 */
	protected MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> getMappingContext() {
		return this.mappingContext;
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

	// -------------------------------------------------------------------------
	// Methods dealing with static CQL
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#select(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> List<T> select(String cql, Class<T> entityClass) {

		Assert.hasText(cql, "Statement must not be empty");

		return select(new SimpleStatement(cql), entityClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#stream(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> Stream<T> stream(String cql, Class<T> entityClass) throws DataAccessException {

		Assert.hasText(cql, "Statement must not be empty");
		Assert.notNull(entityClass, "Entity type must not be null");

		return stream(new SimpleStatement(cql), entityClass);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#selectOne(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> T selectOne(String cql, Class<T> entityClass) {

		Assert.hasText(cql, "Statement must not be empty");
		Assert.notNull(entityClass, "Entity type must not be null");

		return selectOne(new SimpleStatement(cql), entityClass);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.driver.core.Statement
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#select(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> List<T> select(Statement statement, Class<T> entityClass) {

		Assert.notNull(statement, "Statement must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return getCqlOperations().query(statement, (row, rowNum) -> getConverter().read(entityClass, row));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#stream(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> Stream<T> stream(Statement statement, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(statement, "Statement must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return StreamSupport.stream(getCqlOperations().queryForResultSet(statement).spliterator(), false)
				.map(row -> getConverter().read(entityClass, row));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#selectOne(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> T selectOne(Statement statement, Class<T> entityClass) {
		return select(statement, entityClass).stream().findFirst().orElse(null);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with org.springframework.data.cassandra.core.query.Query
	// -------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#select(org.springframework.data.cassandra.core.query.Query, java.lang.Class)
	 */
	@Override
	public <T> List<T> select(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return select(getStatementFactory().select(query, getMappingContext().getRequiredPersistentEntity(entityClass)),
				entityClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#stream(org.springframework.data.cassandra.core.query.Query, java.lang.Class)
	 */
	@Override
	public <T> Stream<T> stream(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return stream(getStatementFactory().select(query, getMappingContext().getRequiredPersistentEntity(entityClass)),
				entityClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#selectOne(org.springframework.data.cassandra.core.query.Query, java.lang.Class)
	 */
	@Override
	public <T> T selectOne(Query query, Class<T> entityClass) throws DataAccessException {

		List<T> result = select(query, entityClass);

		return (result.isEmpty() ? null : result.get(0));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#update(org.springframework.data.cassandra.core.query.Query, org.springframework.data.cassandra.core.query.Update, java.lang.Class)
	 */
	@Override
	public boolean update(Query query, org.springframework.data.cassandra.core.query.Update update, Class<?> entityClass)
			throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(update, "Update must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return getCqlOperations().execute(
				getStatementFactory().update(query, update, getMappingContext().getRequiredPersistentEntity(entityClass)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#delete(org.springframework.data.cassandra.core.query.Query, java.lang.Class)
	 */
	@Override
	public boolean delete(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return getCqlOperations()
				.execute(getStatementFactory().delete(query, getMappingContext().getRequiredPersistentEntity(entityClass)));
	}

	// -------------------------------------------------------------------------
	// Methods dealing with entities
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#count(java.lang.Class)
	 */
	@Override
	public long count(Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");

		Select select = QueryBuilder.select().countAll()
				.from(getMappingContext().getRequiredPersistentEntity(entityClass).getTableName().toCql());

		Long count = getCqlOperations().queryForObject(select, Long.class);

		return count != null ? count : 0L;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#exists(java.lang.Object, java.lang.Class)
	 */
	@Override
	public boolean exists(Object id, Class<?> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getMappingContext().getRequiredPersistentEntity(entityClass);

		Select select = QueryBuilder.select().from(entity.getTableName().toCql());

		getConverter().write(id, select.where(), entity);

		return getCqlOperations().queryForResultSet(select).iterator().hasNext();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#selectOneById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> T selectOneById(Object id, Class<T> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getMappingContext().getRequiredPersistentEntity(entityClass);

		Select select = QueryBuilder.select().all().from(entity.getTableName().toCql());

		getConverter().write(id, select.where(), entity);

		return selectOne(select, entityClass);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.lang.Object)
	 */
	@Override
	public void insert(Object entity) {
		insert(entity, InsertOptions.empty());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.lang.Object, org.springframework.data.cassandra.core.InsertOptions)
	 */
	@Override
	public WriteResult insert(Object entity, InsertOptions options) {

		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(options, "InsertOptions must not be null");

		Insert insert = QueryUtils.createInsertQuery(getTableName(entity.getClass()).toCql(), entity, options, converter);

		// noinspection ConstantConditions
		return getCqlOperations().execute(new StatementCallback(insert));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#update(java.lang.Object)
	 */
	@Override
	public void update(Object entity) {
		update(entity, UpdateOptions.empty());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#update(java.lang.Object, org.springframework.data.cassandra.core.UpdateOptions)
	 */
	@Override
	public WriteResult update(Object entity, UpdateOptions options) {

		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(options, "UpdateOptions must not be null");

		Update update = QueryUtils.createUpdateQuery(getTableName(entity.getClass()).toCql(), entity, options, converter);

		// noinspection ConstantConditions
		return getCqlOperations().execute(new StatementCallback(update));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#delete(java.lang.Object)
	 */
	@Override
	public void delete(Object entity) {
		delete(entity, QueryOptions.empty());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#delete(java.lang.Object, org.springframework.data.cassandra.core.cql.QueryOptions)
	 */
	@Override
	public WriteResult delete(Object entity, QueryOptions options) {

		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(options, "QueryOptions must not be null");

		Delete delete = QueryUtils.createDeleteQuery(getTableName(entity.getClass()).toCql(), entity, options, converter);

		// noinspection ConstantConditions
		return getCqlOperations().execute(new StatementCallback(delete));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#deleteById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public boolean deleteById(Object id, Class<?> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getMappingContext().getRequiredPersistentEntity(entityClass);

		Delete delete = QueryBuilder.delete().from(entity.getTableName().toCql());

		getConverter().write(id, delete.where(), entity);

		return getCqlOperations().execute(delete);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#truncate(java.lang.Class)
	 */
	@Override
	public void truncate(Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");

		Truncate truncate = QueryBuilder
				.truncate(getMappingContext().getRequiredPersistentEntity(entityClass).getTableName().toCql());

		getCqlOperations().execute(truncate);
	}

	// -------------------------------------------------------------------------
	// Implementation hooks and helper methods
	// -------------------------------------------------------------------------

	/*
	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#getTableName(java.lang.Class)
	 */
	@Override
	public CqlIdentifier getTableName(Class<?> entityClass) {
		return getMappingContext().getRequiredPersistentEntity(ClassUtils.getUserClass(entityClass)).getTableName();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#batchOps()
	 */
	@Override
	public CassandraBatchOperations batchOps() {
		return new CassandraBatchTemplate(this);
	}

	@Value
	static class StatementCallback implements SessionCallback<WriteResult>, CqlProvider {

		@NonNull Statement statement;

		@Override
		public WriteResult doInSession(Session session) throws DriverException, DataAccessException {
			return WriteResult.of(session.execute(statement));
		}

		@Override
		public String getCql() {
			return statement.toString();
		}
	}
}

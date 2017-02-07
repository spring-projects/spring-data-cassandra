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

import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.springframework.cassandra.core.CqlOperations;
import org.springframework.cassandra.core.CqlProvider;
import org.springframework.cassandra.core.CqlTemplate;
import org.springframework.cassandra.core.QueryOptions;
import org.springframework.cassandra.core.SessionCallback;
import org.springframework.cassandra.core.WriteOptions;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.cassandra.core.session.DefaultSessionFactory;
import org.springframework.cassandra.core.session.SessionFactory;
import org.springframework.cassandra.core.util.CollectionUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
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

	private final CassandraMappingContext mappingContext;

	private final CqlOperations cqlOperations;

	/**
	 * Creates an instance of {@link CassandraTemplate} initialized with the given {@link Session}
	 * and a default {@link MappingCassandraConverter}.
	 *
	 * @param session {@link Session} used to interact with Cassandra; must not be {@literal null}.
	 * @see CassandraConverter
	 * @see Session
	 */
	public CassandraTemplate(Session session) {
		this(session, newConverter());
	}

	/**
	 * Creates an instance of {@link CassandraTemplate} initialized with the given {@link Session}
	 * and {@link CassandraConverter}.
	 *
	 * @param session {@link Session} used to interact with Cassandra; must not be {@literal null}.
	 * @param converter {@link CassandraConverter} used to convert between Java and Cassandra types;
	 * must not be {@literal null}.
	 * @see CassandraConverter
	 * @see Session
	 */
	public CassandraTemplate(Session session, CassandraConverter converter) {
		this(new DefaultSessionFactory(session), converter);
	}

	/**
	 * Creates an instance of {@link CassandraTemplate} initialized with the given {@link SessionFactory}
	 * and {@link CassandraConverter}.
	 *
	 * @param sessionFactory {@link SessionFactory} used to interact with Cassandra; must not be {@literal null}.
	 * @param converter {@link CassandraConverter} used to convert between Java and Cassandra types;
	 * must not be {@literal null}.
	 * @see CassandraConverter
	 * @see SessionFactory
	 */
	public CassandraTemplate(SessionFactory sessionFactory, CassandraConverter converter) {
		this(new CqlTemplate(sessionFactory), converter);
	}

	/**
	 * Creates an instance of {@link CassandraTemplate} initialized with the given {@link CqlOperations}
	 * and {@link CassandraConverter}.
	 *
	 * @param cqlOperations {@link CqlOperations} used to interact with Cassandra; must not be {@literal null}.
	 * @param converter {@link CassandraConverter} used to convert between Java and Cassandra types;
	 * must not be {@literal null}.
	 * @see CassandraConverter
	 * @see Session
	 */
	public CassandraTemplate(CqlOperations cqlOperations, CassandraConverter converter) {

		Assert.notNull(cqlOperations, "CqlOperations must not be null");
		Assert.notNull(converter, "CassandraConverter must not be null");

		this.converter = converter;
		this.mappingContext = converter.getMappingContext();
		this.cqlOperations = cqlOperations;
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
	 * @see org.springframework.data.cassandra.core.CassandraOperations#select(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> List<T> select(String cql, Class<T> entityClass) {

		Assert.hasText(cql, "Statement must not be empty");

		return select(new SimpleStatement(cql), entityClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperationsNG#stream(java.lang.String, java.lang.Class)
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

		return cqlOperations.query(statement, (row, rowNum) -> converter.read(entityClass, row));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperationsNG#stream(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> Stream<T> stream(Statement statement, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(statement, "Statement must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		return StreamSupport.stream(cqlOperations.queryForResultSet(statement).spliterator(), false)
				.map(row -> converter.read(entityClass, row));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#selectOne(com.datastax.driver.core.Statement, java.lang.Class)
	 */
	@Override
	public <T> T selectOne(Statement statement, Class<T> entityClass) {

		List<T> result = select(statement, entityClass);

		return (result.isEmpty() ? null : result.get(0));
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

		Select select = QueryBuilder.select().countAll().from(getPersistentEntity(entityClass).getTableName().toCql());

		return cqlOperations.queryForObject(select, Long.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#exists(java.lang.Object, java.lang.Class)
	 */
	@Override
	public boolean exists(Object id, Class<?> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getPersistentEntity(entityClass);

		Select select = QueryBuilder.select().from(entity.getTableName().toCql());

		converter.write(id, select.where(), entity);

		return cqlOperations.queryForResultSet(select).iterator().hasNext();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#selectOneById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> T selectOneById(Object id, Class<T> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getPersistentEntity(entityClass);

		Select select = QueryBuilder.select().all().from(entity.getTableName().toCql());

		converter.write(id, select.where(), entity);

		return selectOne(select, entityClass);
	}

	@Override
	public <T> List<T> selectBySimpleIds(Iterable<?> ids, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(ids, "Ids must not be null");
		Assert.notNull(entityClass, "EntityClass must not be null");

		CassandraPersistentEntity<?> entity = getPersistentEntity(entityClass);

		if (entity.getIdProperty() == null || entity.getIdProperty().isCompositePrimaryKey()) {
			String typeName = (entity.getIdProperty() == null ? "Unknown"
					: entity.getIdProperty().getCompositePrimaryKeyEntity().getType().getName());

			throw new IllegalArgumentException(
					String.format("Entity class [%s] uses a composite primary key class [%s] which this method can't support",
							entityClass.getName(), typeName));
		}

		Select select = QueryBuilder.select().all().from(entity.getTableName().toCql());

		select.where(QueryBuilder.in(entity.getIdProperty().getColumnName().toCql(), CollectionUtils.toArray(ids)));

		return select(select, entityClass);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.lang.Object)
	 */
	@Override
	public <T> T insert(T entity) {
		return insert(entity, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.lang.Object, org.springframework.cassandra.core.WriteOptions)
	 */
	@Override
	public <T> T insert(T entity, WriteOptions options) {

		Assert.notNull(entity, "Entity must not be null");

		Insert insert = QueryUtils.createInsertQuery(getTableName(entity.getClass()).toCql(),
				entity, options, converter);

		return cqlOperations.execute(new StatementCallback<>(insert, entity));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#update(java.lang.Object)
	 */
	@Override
	public <T> T update(T entity) {
		return update(entity, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#update(java.lang.Object, org.springframework.cassandra.core.WriteOptions)
	 */
	@Override
	public <T> T update(T entity, WriteOptions options) {

		Assert.notNull(entity, "Entity must not be null");

		Update update = QueryUtils.createUpdateQuery(getTableName(entity.getClass()).toCql(),
				entity, options, converter);

		return cqlOperations.execute(new StatementCallback<>(update, entity));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#delete(java.lang.Object)
	 */
	@Override
	public <T> T delete(T entity) {
		return delete(entity, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#delete(java.lang.Object, org.springframework.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T delete(T entity, QueryOptions options) {

		Assert.notNull(entity, "Entity must not be null");

		Delete delete = QueryUtils.createDeleteQuery(getTableName(entity.getClass()).toCql(),
				entity, options, converter);

		return cqlOperations.execute(new StatementCallback<>(delete, entity));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#deleteById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public boolean deleteById(Object id, Class<?> entityClass) {

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = getPersistentEntity(entityClass);

		Delete delete = QueryBuilder.delete().from(entity.getTableName().toCql());

		converter.write(id, delete.where(), entity);

		return cqlOperations.execute(delete);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#truncate(java.lang.Class)
	 */
	@Override
	public void truncate(Class<?> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");

		Truncate truncate = QueryBuilder.truncate(getPersistentEntity(entityClass).getTableName().toCql());

		cqlOperations.execute(truncate);
	}

	// -------------------------------------------------------------------------
	// Implementation hooks and helper methods
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#getConverter()
	 */
	@Override
	public CassandraConverter getConverter() {
		return converter;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#CqlOperations()
	 */
	@Override
	public CqlOperations getCqlOperations() {
		return cqlOperations;
	}

	protected <T> CassandraPersistentEntity<?> getPersistentEntity(Class<T> entityClass) {

		Assert.notNull(entityClass, "Entity type must not be null");

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		if (entity == null) {
			throw new InvalidDataAccessApiUsageException(
					String.format("No Persistent Entity information found for the class [%s]", entityClass.getName()));
		}

		return entity;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperationsNG#getTableName(java.lang.Class)
	 */
	@Override
	public CqlIdentifier getTableName(Class<?> entityClass) {
		return getPersistentEntity(ClassUtils.getUserClass(entityClass)).getTableName();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperationsNG#batchOps()
	 */
	@Override
	public CassandraBatchOperations batchOps() {
		return new CassandraBatchTemplate(this);
	}

	private static class StatementCallback<T> implements SessionCallback<T>, CqlProvider {

		private final Statement statement;
		private final T entity;

		StatementCallback(Statement statement, T entity) {
			this.statement = statement;
			this.entity = entity;
		}

		@Override
		public T doInSession(Session session) throws DriverException, DataAccessException {
			return session.execute(statement).wasApplied() ? entity : null;
		}

		@Override
		public String getCql() {
			return statement.toString();
		}
	}
}

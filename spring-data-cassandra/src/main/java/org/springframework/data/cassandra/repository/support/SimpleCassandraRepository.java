/*
 * Copyright 2010-2012 the original author or authors.
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
package org.springframework.data.cassandra.repository.support;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraDataOperations;
import org.springframework.data.cassandra.core.CassandraDataTemplate;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.query.CassandraEntityInformation;
import org.springframework.util.Assert;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * Repository base implementation for Cassandra.
 * 
 * @author Alex Shvid
 * 
 */

public class SimpleCassandraRepository<T, ID extends Serializable> implements CassandraRepository<T, ID> {

	private final CassandraOperations operations;
	private final CassandraDataOperations dataOperations;
	private final CassandraEntityInformation<T, ID> entityInformation;

	/**
	 * Creates a new {@link SimpleCassandraRepository} for the given {@link CassandraEntityInformation} and
	 * {@link CassandraDataTemplate}.
	 * 
	 * @param metadata must not be {@literal null}.
	 * @param template must not be {@literal null}.
	 */
	public SimpleCassandraRepository(CassandraEntityInformation<T, ID> metadata, CassandraOperations operations,
			CassandraDataOperations dataOperations) {

		Assert.notNull(operations);
		Assert.notNull(dataOperations);
		Assert.notNull(metadata);

		this.entityInformation = metadata;
		this.operations = operations;
		this.dataOperations = dataOperations;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#save(java.lang.Object)
	 */
	public <S extends T> S save(S entity) {

		Assert.notNull(entity, "Entity must not be null!");

		// INSERT OR OPDATE?

		dataOperations.update(entity, entityInformation.getTableName());
		return entity;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#save(java.lang.Iterable)
	 */
	public <S extends T> List<S> save(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities not be null!");

		List<S> result = new ArrayList<S>();

		for (S entity : entities) {
			save(entity);
			result.add(entity);
		}

		return result;
	}

	private Clause getIdClause(ID id) {
		Clause clause = QueryBuilder.eq(entityInformation.getIdColumn(), id);
		return clause;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findOne(java.io.Serializable)
	 */
	public T findOne(ID id) {
		Assert.notNull(id, "The given id must not be null!");

		Select select = QueryBuilder.select().all().from(entityInformation.getTableName());
		select.where(getIdClause(id));

		return dataOperations.selectOne(select, entityInformation.getJavaType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#exists(java.io.Serializable)
	 */
	public boolean exists(ID id) {

		Assert.notNull(id, "The given id must not be null!");

		Select select = QueryBuilder.select().countAll().from(entityInformation.getTableName());
		select.where(getIdClause(id));

		Long num = dataOperations.count(select);
		return num != null && num.longValue() > 0;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#count()
	 */
	public long count() {
		return dataOperations.count(entityInformation.getTableName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.io.Serializable)
	 */
	public void delete(ID id) {
		Assert.notNull(id, "The given id must not be null!");

		Delete delete = QueryBuilder.delete().all().from(entityInformation.getTableName());
		delete.where(getIdClause(id));

		operations.execute(delete.getQueryString());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.lang.Object)
	 */
	public void delete(T entity) {
		Assert.notNull(entity, "The given entity must not be null!");
		delete(entityInformation.getId(entity));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.lang.Iterable)
	 */
	public void delete(Iterable<? extends T> entities) {

		Assert.notNull(entities, "The given Iterable of entities not be null!");

		for (T entity : entities) {
			delete(entity);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#deleteAll()
	 */
	public void deleteAll() {
		Delete delete = QueryBuilder.delete().all().from(entityInformation.getTableName());
		operations.execute(delete.getQueryString());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findAll()
	 */
	public List<T> findAll() {
		Select select = QueryBuilder.select().all().from(entityInformation.getTableName());
		return findAll(select);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findAll(java.lang.Iterable)
	 */
	public Iterable<T> findAll(Iterable<ID> ids) {

		List<ID> parameters = new ArrayList<ID>();
		for (ID id : ids) {
			parameters.add(id);
		}
		Clause clause = QueryBuilder.in(entityInformation.getIdColumn(), parameters.toArray());
		Select select = QueryBuilder.select().all().from(entityInformation.getTableName());
		select.where(clause);

		return findAll(select);
	}

	private List<T> findAll(Select query) {

		if (query == null) {
			return Collections.emptyList();
		}

		return dataOperations.select(query, entityInformation.getJavaType());
	}

	/**
	 * Returns the underlying {@link CassandraOperations} instance.
	 * 
	 * @return
	 */
	protected CassandraOperations getCassandraOperations() {
		return this.operations;
	}

	/**
	 * Returns the underlying {@link CassandraDataOperations} instance.
	 * 
	 * @return
	 */
	protected CassandraDataOperations getCassandraDataOperations() {
		return this.dataOperations;
	}

	/**
	 * @return the entityInformation
	 */
	protected CassandraEntityInformation<T, ID> getEntityInformation() {
		return entityInformation;
	}

}

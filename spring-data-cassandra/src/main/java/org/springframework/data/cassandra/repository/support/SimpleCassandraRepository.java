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

import org.springframework.cassandra.core.CqlOperations;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.query.CassandraEntityInformation;
import org.springframework.util.Assert;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;

/**
 * Repository base implementation for Cassandra.
 * 
 * @author Alex Shvid
 * 
 */

public class SimpleCassandraRepository<T, ID extends Serializable> implements CassandraRepository<T, ID> {

	protected final CassandraTemplate cassandraTemplate;
	protected final CassandraEntityInformation<T, ID> entityInformation;

	/**
	 * Creates a new {@link SimpleCassandraRepository} for the given {@link CassandraEntityInformation} and
	 * {@link CassandraTemplate}.
	 * 
	 * @param metadata must not be {@literal null}.
	 * @param template must not be {@literal null}.
	 */
	public SimpleCassandraRepository(CassandraEntityInformation<T, ID> metadata, CassandraTemplate cassandraTemplate) {

		Assert.notNull(cassandraTemplate);
		Assert.notNull(metadata);

		this.entityInformation = metadata;
		this.cassandraTemplate = cassandraTemplate;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#save(java.lang.Object)
	 */
	@Override
	public <S extends T> S save(S entity) {

		Assert.notNull(entity, "Entity must not be null!");
		cassandraTemplate.insert(entity, entityInformation.getTableName());
		return entity;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#save(java.lang.Iterable)
	 */
	@Override
	public <S extends T> List<S> save(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities not be null!");

		List<S> result = new ArrayList<S>();

		for (S entity : entities) {
			save(entity);
			result.add(entity);
		}

		return result;
	}

	@Override
	public T findOne(ID id) {
		return cassandraTemplate.selectOneById(entityInformation.getJavaType(), id);
	}

	@Override
	public boolean exists(ID id) {
		return cassandraTemplate.countById(entityInformation.getJavaType(), id) >= 1; // TODO: == instead of >= ?
	}

	@Override
	public long count() {
		return cassandraTemplate.count(entityInformation.getTableName());
	}

	@Override
	public void delete(ID id) {
		cassandraTemplate.deleteById(entityInformation.getJavaType(), id);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.lang.Object)
	 */
	@Override
	public void delete(T entity) {
		Assert.notNull(entity, "The given entity must not be null!");
		delete(entityInformation.getId(entity));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.lang.Iterable)
	 */
	@Override
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
	@Override
	public void deleteAll() {
		cassandraTemplate.truncate(entityInformation.getTableName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findAll()
	 */
	@Override
	public List<T> findAll() {
		Select select = QueryBuilder.select().all().from(entityInformation.getTableName());
		return findAll(select);
	}

	@Override
	public Iterable<T> findAll(Iterable<ID> ids) {

		return cassandraTemplate.selectByIds(entityInformation.getJavaType(), ids);
	}

	protected List<T> findAll(Select query) {

		if (query == null) {
			return Collections.emptyList();
		}

		return cassandraTemplate.select(query, entityInformation.getJavaType());
	}

	/**
	 * Returns the underlying {@link CqlOperations} instance.
	 * 
	 * @return
	 */
	protected CqlOperations getCassandraOperations() {
		return this.cassandraTemplate;
	}

	/**
	 * Returns the underlying {@link CassandraOperations} instance.
	 * 
	 * @return
	 */
	protected CassandraOperations getCassandraDataOperations() {
		return this.cassandraTemplate;
	}

	/**
	 * @return the entityInformation
	 */
	protected CassandraEntityInformation<T, ID> getEntityInformation() {
		return entityInformation;
	}

}

/*
 * Copyright 2013-2016 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.repository.support;

import java.io.Serializable;
import java.util.List;

import org.springframework.cassandra.core.util.CollectionUtils;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.repository.TypedIdCassandraRepository;
import org.springframework.data.cassandra.repository.query.CassandraEntityInformation;
import org.springframework.util.Assert;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * Repository base implementation for Cassandra.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class SimpleCassandraRepository<T, ID extends Serializable> implements TypedIdCassandraRepository<T, ID> {

	private CassandraOperations operations;
	private CassandraEntityInformation<T, ID> entityInformation;

	/**
	 * Creates a new {@link SimpleCassandraRepository} for the given {@link CassandraEntityInformation} and
	 * {@link CassandraTemplate}.
	 * 
	 * @param metadata must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public SimpleCassandraRepository(CassandraEntityInformation<T, ID> metadata, CassandraOperations operations) {

		Assert.notNull(operations);
		Assert.notNull(metadata);

		this.entityInformation = metadata;
		this.operations = operations;
	}

	@Override
	public <S extends T> S save(S entity) {
		return operations.insert(entity);
	}

	@Override
	public <S extends T> List<S> save(Iterable<S> entities) {
		return operations.insert(CollectionUtils.toList(entities));
	}

	@Override
	public T findOne(ID id) {
		return operations.selectOneById(id, entityInformation.getJavaType());
	}

	@Override
	public boolean exists(ID id) {
		return operations.exists(id, entityInformation.getJavaType());
	}

	@Override
	public long count() {
		return operations.count(entityInformation.getJavaType());
	}

	@Override
	public void delete(ID id) {
		operations.deleteById(id, entityInformation.getJavaType());
	}

	@Override
	public void delete(T entity) {
		delete(entityInformation.getId(entity));
	}

	@Override
	public void delete(Iterable<? extends T> entities) {
		operations.delete(CollectionUtils.toList(entities));
	}

	@Override
	public void deleteAll() {
		operations.truncate(entityInformation.getJavaType());
	}

	@Override
	public List<T> findAll() {

		Select select = QueryBuilder.select().all().from(entityInformation.getTableName().toCql());

		return operations.select(select, entityInformation.getJavaType());
	}

	@Override
	public Iterable<T> findAll(Iterable<ID> ids) {
		return operations.selectBySimpleIds(ids, entityInformation.getJavaType());
	}
}

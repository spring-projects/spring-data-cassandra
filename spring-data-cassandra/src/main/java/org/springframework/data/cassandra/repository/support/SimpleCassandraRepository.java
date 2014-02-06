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
import java.util.List;

import org.springframework.cassandra.core.util.CollectionUtils;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.query.CassandraEntityInformation;
import org.springframework.util.Assert;

import com.datastax.driver.core.querybuilder.Select;

/**
 * Repository base implementation for Cassandra.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */
public class SimpleCassandraRepository<T, ID extends Serializable> implements CassandraRepository<T, ID> {

	protected CassandraOperations template;
	protected CassandraEntityInformation<T, ID> entityInformation;

	/**
	 * Creates a new {@link SimpleCassandraRepository} for the given {@link CassandraEntityInformation} and
	 * {@link CassandraTemplate}.
	 * 
	 * @param metadata must not be {@literal null}.
	 * @param template must not be {@literal null}.
	 */
	public SimpleCassandraRepository(CassandraEntityInformation<T, ID> metadata, CassandraTemplate template) {

		Assert.notNull(template);
		Assert.notNull(metadata);

		this.entityInformation = metadata;
		this.template = template;
	}

	@Override
	public <S extends T> S save(S entity) {
		return template.insert(entity);
	}

	@Override
	public <S extends T> List<S> save(Iterable<S> entities) {
		return template.insert(CollectionUtils.toList(entities));
	}

	@Override
	public T findOne(ID id) {
		return template.selectOneById(entityInformation.getJavaType(), id);
	}

	@Override
	public boolean exists(ID id) {
		return template.exists(entityInformation.getJavaType(), id);
	}

	@Override
	public long count() {
		return template.count(entityInformation.getTableName());
	}

	@Override
	public void delete(ID id) {
		template.deleteById(entityInformation.getJavaType(), id);
	}

	@Override
	public void delete(T entity) {
		delete(entityInformation.getId(entity));
	}

	@Override
	public void delete(Iterable<? extends T> entities) {
		template.delete(CollectionUtils.toList(entities));
	}

	@Override
	public void deleteAll() {
		template.truncate(entityInformation.getTableName());
	}

	@Override
	public List<T> findAll() {
		return template.selectAll(entityInformation.getJavaType());
	}

	@Override
	public Iterable<T> findAll(Iterable<ID> ids) {
		return template.selectBySimpleIds(entityInformation.getJavaType(), ids);
	}

	protected List<T> findAll(Select query) {
		return template.select(query.getQueryString(), entityInformation.getJavaType());
	}
}

/*
 * Copyright 2013-2014 the original author or authors
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
package org.springframework.data.cassandra.core;

import java.util.List;

import org.springframework.cassandra.core.CqlOperations;
import org.springframework.cassandra.core.QueryOptions;
import org.springframework.cassandra.core.WriteOptions;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.convert.CassandraConverter;

import com.datastax.driver.core.querybuilder.Select;

/**
 * Operations for interacting with Cassandra. These operations are used by the Repository implementation, but can also
 * be used directly when that is desired by the developer.
 * 
 * @author Alex Shvid
 * @author David Webb
 * @author Matthew Adams
 */
public interface CassandraOperations extends CqlOperations {

	/**
	 * The table name used for the specified class by this template.
	 * 
	 * @param entityClass must not be {@literal null}.
	 * @return
	 */
	CqlIdentifier getTableName(Class<?> entityClass);

	/**
	 * Execute query and convert ResultSet to the list of entities
	 * 
	 * @param query must not be {@literal null}.
	 * @param type must not be {@literal null}, mapped entity type.
	 * @return
	 */
	<T> List<T> select(String cql, Class<T> type);

	/**
	 * Execute the Select Query and convert to the list of entities
	 * 
	 * @param select must not be {@literal null}.
	 * @param type must not be {@literal null}, mapped entity type.
	 * @return
	 */
	<T> List<T> select(Select select, Class<T> type);

	<T> T selectOneById(Class<T> type, Object id);

	/**
	 * Execute CQL and convert ResultSet to the entity
	 * 
	 * @param query must not be {@literal null}.
	 * @param type must not be {@literal null}, mapped entity type.
	 * @return
	 */
	<T> T selectOne(String cql, Class<T> type);

	/**
	 * Execute Select query and convert ResultSet to the entity
	 * 
	 * @param query must not be {@literal null}.
	 * @param type must not be {@literal null}, mapped entity type.
	 * @return
	 */
	<T> T selectOne(Select select, Class<T> type);

	boolean exists(Class<?> type, Object id);

	long count(Class<?> type);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param entity
	 */
	<T> T insert(T entity);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T insert(T entity, WriteOptions options);

	/**
	 * Insert the given list of objects to the table by annotation table name.
	 * 
	 * @param entities
	 * @return
	 */
	<T> List<T> insert(List<T> entities);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> List<T> insert(List<T> entities, WriteOptions options);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> T insertAsynchronously(T entity);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T insertAsynchronously(T entity, WriteOptions options);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> List<T> insertAsynchronously(List<T> entities);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> List<T> insertAsynchronously(List<T> entities, WriteOptions options);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> T update(T entity);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T update(T entity, WriteOptions options);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> List<T> update(List<T> entities);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> List<T> update(List<T> entities, WriteOptions options);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> T updateAsynchronously(T entity);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T updateAsynchronously(T entity, WriteOptions options);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> List<T> updateAsynchronously(List<T> entities);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> List<T> updateAsynchronously(List<T> entities, WriteOptions options);

	/**
	 * Remove the given object from the table by id.
	 * 
	 * @param object
	 */
	<T> void delete(T entity);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 */
	<T> void delete(T entity, QueryOptions options);

	/**
	 * Remove the given object from the table by id.
	 * 
	 * @param object
	 */
	<T> void delete(List<T> entities);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 */
	<T> void delete(List<T> entities, QueryOptions options);

	/**
	 * Deletes all entities of a given class.
	 */
	<T> void deleteAll(Class<T> clazz);

	/**
	 * Remove the given object from the table by id.
	 * 
	 * @param object
	 */
	<T> void deleteAsynchronously(T entity);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 */
	<T> void deleteAsynchronously(T entity, QueryOptions options);

	/**
	 * Remove the given object from the table by id.
	 * 
	 * @param object
	 */
	<T> void deleteAsynchronously(List<T> entities);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 */
	<T> void deleteAsynchronously(List<T> entities, QueryOptions options);

	/**
	 * Returns the underlying {@link CassandraConverter}.
	 * 
	 * @return
	 */
	CassandraConverter getConverter();

	void deleteById(Class<?> type, Object id);

	<T> List<T> selectBySimpleIds(Class<T> type, Iterable<?> ids);

	<T> List<T> selectAll(Class<T> type);
}

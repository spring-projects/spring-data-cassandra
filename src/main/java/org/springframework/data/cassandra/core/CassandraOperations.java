/*
 * Copyright 2011-2013 the original author or authors.
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
import java.util.Map;

import org.springframework.data.cassandra.convert.CassandraConverter;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;

/**
 * Main Inteface that should be used for Cassandra interactions
 * 
 * @author Alex Shvid
 * @author David Webb
 * 
 */
public interface CassandraOperations {

	/**
	 * Describe the current Ring
	 * 
	 * @return The list of ring tokens that are active in the cluster
	 */
	List<RingMember> describeRing();

	/**
	 * The table name used for the specified class by this template.
	 * 
	 * @param entityClass must not be {@literal null}.
	 * @return
	 */
	String getTableName(Class<?> entityClass);

	/**
	 * Execute query and return Cassandra ResultSet
	 * 
	 * @param query must not be {@literal null}.
	 * @return
	 */
	ResultSet executeQuery(final String query);

	/**
	 * Execute async query and return Cassandra ResultSetFuture
	 * 
	 * @param query must not be {@literal null}.
	 * @return
	 */
	ResultSetFuture executeQueryAsynchronously(final String query);

	/**
	 * Execute query and convert ResultSet to the list of entities
	 * 
	 * @param query must not be {@literal null}.
	 * @param selectClass must not be {@literal null}, mapped entity type.
	 * @return
	 */
	<T> List<T> select(String query, Class<T> selectClass);

	/**
	 * Execute query and convert ResultSet to the entity
	 * 
	 * @param query must not be {@literal null}.
	 * @param selectClass must not be {@literal null}, mapped entity type.
	 * @return
	 */
	<T> T selectOne(String query, Class<T> selectClass);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param entity
	 */
	<T> T insert(T entity);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param entity
	 * @param tableName
	 * @return
	 */
	<T> T insert(T entity, String tableName);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T insert(T entity, String tableName, QueryOptions options);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T insert(T entity, QueryOptions options);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T insert(T entity, Map<String, Object> optionsByName);

	/**
	 * @param entity
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> T insert(T entity, String tableName, Map<String, Object> optionsByName);

	/**
	 * Insert the given list of objects to the table by annotation table name.
	 * 
	 * @param entities
	 * @return
	 */
	<T> List<T> insert(List<T> entities);

	/**
	 * Insert the given list of objects to the table by name.
	 * 
	 * @param entities
	 * @param tableName
	 * @return
	 */
	<T> List<T> insert(List<T> entities, String tableName);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> List<T> insert(List<T> entities, QueryOptions options);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> List<T> insert(List<T> entities, Map<String, Object> optionsByName);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> List<T> insert(List<T> entities, String tableName, QueryOptions options);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> List<T> insert(List<T> entities, String tableName, Map<String, Object> optionsByName);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> T insertAsynchronously(T entity);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> T insertAsynchronously(T entity, String tableName);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T insertAsynchronously(T entity, QueryOptions options);

	/**
	 * @param entity
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> T insertAsynchronously(T entity, Map<String, Object> optionsByName);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T insertAsynchronously(T entity, String tableName, QueryOptions options);

	/**
	 * @param entity
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> T insertAsynchronously(T entity, String tableName, Map<String, Object> optionsByName);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> List<T> insertAsynchronously(List<T> entities);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> List<T> insertAsynchronously(List<T> entities, String tableName);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> List<T> insertAsynchronously(List<T> entities, QueryOptions options);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> List<T> insertAsynchronously(List<T> entities, Map<String, Object> optionsByName);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> List<T> insertAsynchronously(List<T> entities, String tableName, QueryOptions options);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> List<T> insertAsynchronously(List<T> entities, String tableName, Map<String, Object> optionsByName);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> T update(T entity);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> T update(T entity, String tableName);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T update(T entity, QueryOptions options);

	/**
	 * @param entity
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> T update(T entity, Map<String, Object> optionsByName);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T update(T entity, String tableName, QueryOptions options);

	/**
	 * @param entity
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> T update(T entity, String tableName, Map<String, Object> optionsByName);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> List<T> update(List<T> entities);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> List<T> update(List<T> entities, String tableName);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> List<T> update(List<T> entities, QueryOptions options);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> List<T> update(List<T> entities, Map<String, Object> optionsByName);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> List<T> update(List<T> entities, String tableName, QueryOptions options);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> List<T> update(List<T> entities, String tableName, Map<String, Object> optionsByName);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> T updateAsynchronously(T entity);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> T updateAsynchronously(T entity, String tableName);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T updateAsynchronously(T entity, QueryOptions options);

	/**
	 * @param entity
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> T updateAsynchronously(T entity, Map<String, Object> optionsByName);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T updateAsynchronously(T entity, String tableName, QueryOptions options);

	/**
	 * @param entity
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> T updateAsynchronously(T entity, String tableName, Map<String, Object> optionsByName);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> List<T> updateAsynchronously(List<T> entities);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> List<T> updateAsynchronously(List<T> entities, String tableName);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> List<T> updateAsynchronously(List<T> entities, QueryOptions options);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> List<T> updateAsynchronously(List<T> entities, Map<String, Object> optionsByName);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> List<T> updateAsynchronously(List<T> entities, String tableName, QueryOptions options);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> List<T> updateAsynchronously(List<T> entities, String tableName, Map<String, Object> optionsByName);

	/**
	 * Remove the given object from the table by id.
	 * 
	 * @param object
	 */
	<T> void delete(T entity);

	/**
	 * Removes the given object from the given table.
	 * 
	 * @param object
	 * @param table must not be {@literal null} or empty.
	 */
	<T> void delete(T entity, String tableName);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 */
	<T> void delete(T entity, QueryOptions options);

	/**
	 * @param entity
	 * @param tableName
	 * @param optionsByName
	 */
	<T> void delete(T entity, Map<String, Object> optionsByName);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 */
	<T> void delete(T entity, String tableName, QueryOptions options);

	/**
	 * @param entity
	 * @param tableName
	 * @param optionsByName
	 */
	<T> void delete(T entity, String tableName, Map<String, Object> optionsByName);

	/**
	 * Remove the given object from the table by id.
	 * 
	 * @param object
	 */
	<T> void delete(List<T> entities);

	/**
	 * Removes the given object from the given table.
	 * 
	 * @param object
	 * @param table must not be {@literal null} or empty.
	 */
	<T> void delete(List<T> entities, String tableName);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 */
	<T> void delete(List<T> entities, QueryOptions options);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsByName
	 */
	<T> void delete(List<T> entities, Map<String, Object> optionsByName);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 */
	<T> void delete(List<T> entities, String tableName, QueryOptions options);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsByName
	 */
	<T> void delete(List<T> entities, String tableName, Map<String, Object> optionsByName);

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
	 * @param entity
	 * @param tableName
	 * @param optionsByName
	 */
	<T> void deleteAsynchronously(T entity, Map<String, Object> optionsByName);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 */
	<T> void deleteAsynchronously(T entity, String tableName, QueryOptions options);

	/**
	 * @param entity
	 * @param tableName
	 * @param optionsByName
	 */
	<T> void deleteAsynchronously(T entity, String tableName, Map<String, Object> optionsByName);

	/**
	 * Removes the given object from the given table.
	 * 
	 * @param object
	 * @param table must not be {@literal null} or empty.
	 */
	<T> void deleteAsynchronously(T entity, String tableName);

	/**
	 * Remove the given object from the table by id.
	 * 
	 * @param object
	 */
	<T> void deleteAsynchronously(List<T> entities);

	/**
	 * Removes the given object from the given table.
	 * 
	 * @param object
	 * @param table must not be {@literal null} or empty.
	 */
	<T> void deleteAsynchronously(List<T> entities, String tableName);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 */
	<T> void deleteAsynchronously(List<T> entities, QueryOptions options);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsByName
	 */
	<T> void deleteAsynchronously(List<T> entities, Map<String, Object> optionsByName);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 */
	<T> void deleteAsynchronously(List<T> entities, String tableName, QueryOptions options);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsByName
	 */
	<T> void deleteAsynchronously(List<T> entities, String tableName, Map<String, Object> optionsByName);

	/**
	 * Returns the underlying {@link CassandraConverter}.
	 * 
	 * @return
	 */
	CassandraConverter getConverter();
}

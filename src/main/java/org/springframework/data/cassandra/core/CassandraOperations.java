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

import org.springframework.data.cassandra.convert.CassandraConverter;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.Update;

/**
 * @author Alex Shvid
 */
public interface CassandraOperations {

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
	ResultSet executeQuery(String query);
	
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
	 * @param object
	 */
    void insert(Object entity);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
    void insert(Object entity, String tableName);
    
	/**
	 * Remove the given object from the table by id.
	 * 
	 * @param object
	 */
	void remove(Object object);

	/**
	 * Removes the given object from the given table.
	 * 
	 * @param object
	 * @param table must not be {@literal null} or empty.
	 */
	void remove(Object object, String tableName);
    
	/**
	 * Create a table with the name and fields indicated by the entity class
	 * 
	 * @param entityClass class that determines metadata of the table to create/drop.
	 */
    void createTable(Class<?> entityClass);
    
	/**
	 * Create a table with the name and fields indicated by the entity class
	 * 
	 * @param entityClass class that determines metadata of the table to create/drop.
	 * @param tableName explicit name of the table
	 */
    void createTable(Class<?> entityClass, String tableName);
    
	/**
	 * Alter table with the name and fields indicated by the entity class
	 * 
	 * @param entityClass class that determines metadata of the table to create/drop.
	 */    
    void alterTable(Class<?> entityClass);
    
	/**
	 * Alter table with the name and fields indicated by the entity class
	 * 
	 * @param entityClass class that determines metadata of the table to create/drop.
	 * @param tableName explicit name of the table
	 */    
    void alterTable(Class<?> entityClass, String tableName);    

	/**
	 * Alter table with the name and fields indicated by the entity class
	 * 
	 * @param entityClass class that determines metadata of the table to create/drop.
	 */   
    void dropTable(Class<?> entityClass);

	/**
	 * Alter table with the name and fields indicated by the entity class
	 * 
	 * @param tableName explicit name of the table.
	 */   
    void dropTable(String tableName);
    
	/**
	 * Returns the underlying {@link CassandraConverter}.
	 * 
	 * @return
	 */
    CassandraConverter getConverter();
}

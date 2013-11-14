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

import java.util.Map;

import com.datastax.driver.core.TableMetadata;

/**
 * @author David Webb (dwebb@brightmove.com)
 * 
 */
public interface CassandraAdminOperations {

	/**
	 * Get the Table Meta Data from Cassandra
	 * 
	 * @param entityClass
	 * @param tableName
	 * @return
	 */
	TableMetadata getTableMetadata(Class<?> entityClass, String tableName);

	/**
	 * Create a table with the name and fields indicated by the entity class
	 * 
	 * @param ifNotExists
	 * @param tableName
	 * @param entityClass
	 * @param optionsByName
	 */
	void createTable(boolean ifNotExists, String tableName, Class<?> entityClass, Map<String, Object> optionsByName);

	/**
	 * Alter table with the name and fields indicated by the entity class
	 * 
	 * @param entityClass class that determines metadata of the table to create/drop.
	 * @param tableName explicit name of the table
	 */
	void alterTable(String tableName, Class<?> entityClass, boolean dropRemovedAttributeColumns);

	/**
	 * @param tableName
	 * @param entityClass
	 */
	void replaceTable(String tableName, Class<?> entityClass);

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

}

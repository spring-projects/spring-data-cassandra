/**
 * All BrightMove Code is Copyright 2004-2013 BrightMove Inc.
 * Modification of code without the express written consent of
 * BrightMove, Inc. is strictly forbidden.
 *
 * Author: David Webb (dwebb@brightmove.com)
 * Created On: Nov 13, 2013 
 */
package org.springframework.data.cassandra.core;

import java.util.Map;

/**
 * @author David Webb (dwebb@brightmove.com)
 * 
 */
public interface CassandraAdminOperations {

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

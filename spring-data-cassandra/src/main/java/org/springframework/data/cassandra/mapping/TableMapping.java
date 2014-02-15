/*******************************************************************************
 * Copyright 2013-2014 the original author or authors.
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
 ******************************************************************************/
package org.springframework.data.cassandra.mapping;

import java.util.Set;

/**
 * Interface that stores information about the mapping between a table and the entity or the entities that are mapped to
 * it.
 * 
 * @author Matthew T. Adams
 */
public interface TableMapping {

	/**
	 * Convenience method to return the only member of the set of entity classes. This method can be used when the caller
	 * knows that there is only one entity class in this mapping. If there are multiple and this method is called, an
	 * {@link IllegalStateException} is thrown.
	 */
	Class<?> getEntityClass();

	/**
	 * Convenience method to set this mapping to use a single entity class.
	 * 
	 * @param entityClass The class; may not be null.
	 */
	void setEntityClass(Class<?> entityClass);

	/**
	 * Sets the set of entity classes of this mapping.
	 */
	void setEntityClasses(Set<Class<?>> entityClasses);

	/**
	 * Returns the set of entity classes of this mapping. Never returns null.
	 */
	Set<Class<?>> getEntityClasses();

	/**
	 * Returns the name of this mapping. Never returns null.
	 */
	String getTableName();

	/**
	 * Sets the table name of this mapping.
	 */
	void setTableName(String tableName);
}

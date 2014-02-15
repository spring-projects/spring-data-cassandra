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

import java.util.HashMap;
import java.util.Map;

/**
 * Mapping information for an individual entity class.
 * 
 * @author Matthew T. Adams
 */
public class EntityMapping {

	/**
	 * The name of the entity's class.
	 */
	protected String entityClassName;

	/**
	 * The name of the table to which the entity is mapped.
	 */
	protected String tableName;

	/**
	 * The {@link PropertyMapping}s for each persistent property, keyed on property name.
	 */
	protected Map<String, PropertyMapping> propertyMappings = new HashMap<String, PropertyMapping>();

	public EntityMapping(String entityClassName, String tableName) {
		setEntityClassName(entityClassName);
		setTableName(tableName);
	}

	public String getEntityClassName() {
		return entityClassName;
	}

	public void setEntityClassName(String entityClassName) {
		this.entityClassName = entityClassName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public boolean equals(Object that) {
		if (that == null) {
			return false;
		}
		if (this == that) {
			return true;
		}
		if (!(that instanceof EntityMapping)) {
			return false;
		}

		EntityMapping thatMapping = (EntityMapping) that;

		return this.entityClassName.equals(thatMapping.entityClassName) && this.tableName.equals(thatMapping.tableName);
	}

	@Override
	public int hashCode() {
		return entityClassName.hashCode() ^ tableName.hashCode();
	}
}

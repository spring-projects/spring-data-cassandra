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
package org.springframework.data.cassandra.mapping;

import static org.springframework.cassandra.core.cql.CqlIdentifier.cqlId;
import static org.springframework.cassandra.core.cql.CqlIdentifier.quotedCqlId;

import java.util.HashMap;
import java.util.Map;

import org.springframework.util.Assert;

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
	 * Whether to force the table name to be quoted.
	 */
	protected boolean forceQuote = false;

	/**
	 * The {@link PropertyMapping}s for each persistent property, keyed on property name.
	 */
	protected Map<String, PropertyMapping> propertyMappings = new HashMap<String, PropertyMapping>();

	public EntityMapping(String entityClassName, String tableName) {
		this(entityClassName, tableName, false);
	}

	public EntityMapping(String entityClassName, String tableName, boolean forceQuote) {

		setEntityClassName(entityClassName);
		setTableName(tableName);
		setForceQuote(forceQuote);
	}

	public String getEntityClassName() {
		return entityClassName;
	}

	public void setEntityClassName(String entityClassName) {

		Assert.hasText(entityClassName);
		this.entityClassName = entityClassName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {

		Assert.hasText(tableName);
		this.tableName = tableName;
	}

	public boolean getForceQuote() {
		return forceQuote;
	}

	public void setForceQuote(boolean forceQuote) {
		this.forceQuote = forceQuote;
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

		EntityMapping other = (EntityMapping) that;

		return this.entityClassName.equals(other.entityClassName)
				&& (forceQuote ? quotedCqlId(this.tableName) : cqlId(this.tableName)).equals(other.tableName);
	}

	@Override
	public int hashCode() {
		return entityClassName.hashCode() ^ (forceQuote ? quotedCqlId(this.tableName) : cqlId(this.tableName)).hashCode();
	}
}

/*
 * Copyright 2013-2017 the original author or authors
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Mapping information for an individual entity class.
 *
 * @author Matthew T. Adams
 * @author John Blum
 */
public class EntityMapping {

	/**
	 * The {@link PropertyMapping}s for each persistent property, keyed on property name.
	 */
	private Map<String, PropertyMapping> propertyMappings = Collections.emptyMap();

	/**
	 * The name of the entity's class.
	 */
	private String entityClassName;

	/**
	 * Whether to force the table name to be quoted.
	 */
	private String forceQuote = "false";

	/**
	 * The name of the table to which the entity is mapped.
	 */
	private String tableName = "";

	public EntityMapping(String entityClassName, String tableName) {
		this(entityClassName, tableName, Boolean.FALSE.toString());
	}

	public EntityMapping(String entityClassName, String tableName, String forceQuote) {
		setEntityClassName(entityClassName);
		setTableName(tableName);
		setForceQuote(forceQuote);
	}

	public String getEntityClassName() {
		return entityClassName;
	}

	public void setEntityClassName(String entityClassName) {

		Assert.hasText(entityClassName, "Entity class name must not be null or empty");
		this.entityClassName = entityClassName;
	}

	public String getForceQuote() {
		return forceQuote;
	}

	public void setForceQuote(String forceQuote) {

		Assert.notNull(forceQuote, "Force quote must not be null or empty");
		this.forceQuote = forceQuote;
	}

	public Map<String, PropertyMapping> getPropertyMappings() {
		return Collections.unmodifiableMap(propertyMappings);
	}

	public void setPropertyMappings(Map<String, PropertyMapping> propertyMappings) {
		this.propertyMappings = (propertyMappings != null ? new HashMap<String, PropertyMapping>(propertyMappings)
				: Collections.<String, PropertyMapping> emptyMap());
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {

		Assert.notNull(tableName, "Table name must not be null or empty");
		this.tableName = tableName;
	}

	/**
	 * @inheritDoc
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (!(obj instanceof EntityMapping)) {
			return false;
		}

		EntityMapping that = (EntityMapping) obj;

		return ObjectUtils.nullSafeEquals(this.getEntityClassName(), that.getEntityClassName())
				&& ObjectUtils.nullSafeEquals(this.getForceQuote(), that.getForceQuote())
				&& ObjectUtils.nullSafeEquals(this.getTableName(), that.getTableName());
	}

	/**
	 * @inheritDoc
	 */
	@Override
	public int hashCode() {
		int hashValue = 17;
		hashValue = 37 * hashValue + ObjectUtils.nullSafeHashCode(this.getEntityClassName());
		hashValue = 37 * hashValue + ObjectUtils.nullSafeHashCode(this.getForceQuote());
		hashValue = 37 * hashValue + ObjectUtils.nullSafeHashCode(this.getTableName());
		return hashValue;
	}

	/**
	 * @inheritDoc
	 */
	@Override
	public String toString() {
		return String.format(
				"{ @type = %1$s, entityClassName = %2$s, tableName = %3$s, forceQuote = %4$s, propertyMappings = %5$s }",
				getClass().getName(), getEntityClassName(), getTableName(), getForceQuote(), toString(getPropertyMappings()));
	}

	/* (non-Javadoc) */
	private String toString(Map<?, ?> map) {
		StringBuilder builder = new StringBuilder("[");
		int count = 0;

		for (Map.Entry<?, ?> entry : map.entrySet()) {
			builder.append(++count > 1 ? ", " : "");
			builder.append(String.format("%1$s = %2$s", entry.getKey(), entry.getValue()));
		}

		return builder.toString();
	}
}
